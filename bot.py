import asyncio
import logging
from datetime import datetime, timedelta

from aiogram import Bot, Dispatcher, F
from aiogram.exceptions import TelegramNetworkError
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
)

from config import BOT_TOKEN, CHANNEL_ID, OWNER_ID, DB_PATH
from database import Database
from scheduler import moscow_now, parse_publish_datetime_msk, to_iso

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

dp = Dispatcher()
db = Database(DB_PATH)
db.ensure_owner(OWNER_ID)


class CreateTrainingStates(StatesGroup):
    waiting_for_date = State()
    waiting_for_time = State()
    waiting_for_capacity = State()
    waiting_for_level = State()
    waiting_for_location = State()
    waiting_for_publish_time = State()
    waiting_for_confirm = State()


class CloseTrainingStates(StatesGroup):
    waiting_for_post_confirm = State()


class EditTrainingStates(StatesGroup):
    waiting_for_capacity = State()


class AdminPanelStates(StatesGroup):
    waiting_for_kick_number = State()
    waiting_for_ban_user_id = State()
    waiting_for_ban_days = State()
    waiting_for_ban_reason = State()
    waiting_for_unban_user_id = State()
    waiting_for_add_admin_user_id = State()
    waiting_for_remove_admin_user_id = State()
    waiting_for_allow_reregister_user_id = State()
    waiting_for_disallow_reregister_user_id = State()


def normalize_spaces(text: str) -> str:
    return " ".join(text.strip().split())


def is_admin(user_id: int) -> bool:
    return db.is_admin(user_id)


def is_owner(user_id: int) -> bool:
    return db.is_owner(user_id)


def parse_date(value: str) -> str | None:
    value = normalize_spaces(value)
    try:
        parsed = datetime.strptime(value, "%d.%m.%Y")
        return parsed.strftime("%d.%m.%Y")
    except ValueError:
        return None


def parse_time(value: str) -> str | None:
    value = normalize_spaces(value)
    try:
        parsed = datetime.strptime(value, "%H:%M")
        return parsed.strftime("%H:%M")
    except ValueError:
        return None


def parse_capacity(value: str) -> int | None:
    value = normalize_spaces(value)
    if not value.isdigit():
        return None

    number = int(value)
    if number <= 0 or number > 200:
        return None
    return number


def parse_fio(text: str) -> str | None:
    cleaned = normalize_spaces(text)
    lower = cleaned.lower()

    if not lower.startswith("секция "):
        return None

    fio = cleaned[7:].strip()
    words = fio.split()

    if len(words) < 2 or len(words) > 4:
        return None

    for word in words:
        if len(word) < 2:
            return None

    return fio


def build_training_brief(training: dict) -> str:
    location = training.get("location")
    location_line = f"Место: {location}" if location else "Место: не указано"
    return (
        f"Дата: {training['training_date']}\n"
        f"Время: {training['training_time']}\n"
        f"{location_line}\n"
        f"Уровень: {training['level']}\n"
        f"Лимит мест: {training['capacity']}"
    )


def build_channel_post(training: dict) -> str:
    return (
        "🏐 Открыта запись на тренировку по волейболу\n\n"
        f"{build_training_brief(training)}\n\n"
        "Чтобы записаться, напишите в сообщения каналу:\n"
        "Секция ФИО\n\n"
        "Примеры команд:\n"
        "• Секция Иванов Иван Иванович\n"
        "• Инфо\n"
        "• Мой номер\n"
        "• Отмена"
    )


def build_channel_close_post(training: dict) -> str:
    return (
        "⛔ Запись на тренировку закрыта\n\n"
        f"{build_training_brief(training)}"
    )


def format_counts(training: dict, counts: dict) -> str:
    free = max(training["capacity"] - counts["active"], 0)
    return (
        f"Основной список: {counts['active']}/{training['capacity']}\n"
        f"Свободных мест: {free}\n"
        f"Лист ожидания: {counts['waiting']}"
    )


def render_registrations(title: str, items: list[dict]) -> str:
    if not items:
        return f"{title}\nПусто."

    lines = [title]
    for item in items:
        if item.get("username"):
            user_part = f"@{item['username']}"
        else:
            user_part = f"id {item['user_id']}"
        lines.append(f"{item['queue_number']}. {item['fio']} ({user_part})")
    return "\n".join(lines)


def users_can_register(training: dict | None) -> bool:
    if not training:
        return False
    return training.get("status") == "open"


def scheduled_registration_text(training: dict) -> str:
    publish_at = training.get("publish_at")
    when = f"Публикация (MSK): {publish_at}" if publish_at else "Публикация: вручную"
    return (
        "ℹ️ Запись открыта.\n"
        "Пост в канале публикуется вручную администратором (возможно, позже).\n"
        "Это не мешает записываться через сообщения каналу.\n\n"
        f"{build_training_brief(training)}\n"
        f"{when}\n\n"
        "Для записи отправьте:\n"
        "Секция ФИО"
    )


async def notify_admins(bot: Bot, text: str, exclude_user_id: int | None = None):
    admins = db.list_admins()
    for admin in admins:
        admin_id = admin["user_id"]
        if exclude_user_id and admin_id == exclude_user_id:
            continue
        try:
            await bot.send_message(chat_id=admin_id, text=text)
        except Exception as e:
            logging.warning("Не удалось отправить уведомление админу %s: %s", admin_id, e)


async def reply_to_channel_dm(bot: Bot, message: Message, text: str):
    if not message.direct_messages_topic:
        return
    await bot.send_message(
        chat_id=message.chat.id,
        direct_messages_topic_id=message.direct_messages_topic.topic_id,
        text=text,
    )


async def send_to_dm_topic(bot: Bot, dm_chat_id: int, dm_topic_id: int, text: str):
    await bot.send_message(
        chat_id=dm_chat_id,
        direct_messages_topic_id=dm_topic_id,
        text=text,
    )


def private_help_text(user_id: int) -> str:
    base = [
        "Команды:",
        "/my_id — показать ваш user_id",
    ]

    if is_admin(user_id):
        base.extend(
            [
                "/panel — панель управления (кнопки)",
                "/hide_panel — скрыть панель кнопок",
                "/new_training — открыть новую запись (сразу или по расписанию)",
                "/close_training — закрыть текущую запись",
                "/training — показать текущую тренировку",
                "/list — основной список",
                "/waiting — лист ожидания",
                "/edit_training — изменить лимит мест",
                "/kick a3 — удалить №3 из основного списка",
                "/kick w3 — удалить №3 из листа ожидания",
                "/ban 123456789 7 причина — забанить (дни=0/нет → навсегда)",
                "/unban 123456789 — разбанить",
                "/bans — список банов",
                "/allow_reregister 123456789 — разрешить повторную запись на текущую тренировку",
                "/disallow_reregister 123456789 — запретить повторную запись на текущую тренировку",
                "/admins — список админов",
                "/cancel — отменить текущий пошаговый ввод",
            ]
        )

    if is_owner(user_id):
        base.extend(
            [
                "/add_admin 123456789 — добавить админа",
                "/remove_admin 123456789 — удалить админа",
            ]
        )

    return "\n".join(base)


def build_admin_panel_kb(user_id: int) -> InlineKeyboardMarkup:
    keyboard: list[list[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton(text="ℹ️ Тренировка", callback_data="ap:training"),
            InlineKeyboardButton(text="🆔 Мой id", callback_data="ap:my_id"),
        ],
        [
            InlineKeyboardButton(text="➕ Новая запись", callback_data="ap:new_training"),
            InlineKeyboardButton(text="⛔ Закрыть запись", callback_data="ap:close_training"),
        ],
        [
            InlineKeyboardButton(text="✏️ Лимит мест", callback_data="ap:edit_training"),
            InlineKeyboardButton(text="👮 Админы", callback_data="ap:admins"),
        ],
        [
            InlineKeyboardButton(text="📋 Основной список", callback_data="ap:list_active"),
            InlineKeyboardButton(text="🕒 Лист ожидания", callback_data="ap:list_waiting"),
        ],
        [
            InlineKeyboardButton(text="🗑 Удалить из основного", callback_data="ap:kick_active"),
            InlineKeyboardButton(text="🗑 Удалить из ожидания", callback_data="ap:kick_waiting"),
        ],
        [
            InlineKeyboardButton(text="⛔ Бан", callback_data="ap:ban"),
            InlineKeyboardButton(text="✅ Разбан", callback_data="ap:unban"),
            InlineKeyboardButton(text="📄 Баны", callback_data="ap:bans"),
        ],
        [
            InlineKeyboardButton(text="❓ Команды", callback_data="ap:help"),
        ],
    ]

    if is_owner(user_id):
        keyboard.insert(
            3,
            [
                InlineKeyboardButton(text="➕ Добавить админа", callback_data="ap:add_admin"),
                InlineKeyboardButton(text="➖ Удалить админа", callback_data="ap:remove_admin"),
            ],
        )

    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def build_admin_panel_reply_kb(user_id: int) -> ReplyKeyboardMarkup:
    rows: list[list[KeyboardButton]] = [
        [
            KeyboardButton(text="/training"),
            KeyboardButton(text="/list"),
            KeyboardButton(text="/waiting"),
        ],
        [
            KeyboardButton(text="/new_training"),
            KeyboardButton(text="/close_training"),
        ],
        [
            KeyboardButton(text="/edit_training"),
            KeyboardButton(text="🗑 Кик (основной)"),
            KeyboardButton(text="🗑 Кик (ожидание)"),
        ],
        [
            KeyboardButton(text="⛔ Бан"),
            KeyboardButton(text="✅ Разбан"),
            KeyboardButton(text="/bans"),
        ],
        [
            KeyboardButton(text="✅ Разрешить перезапись"),
            KeyboardButton(text="⛔ Запретить перезапись"),
        ],
        [
            KeyboardButton(text="/admins"),
            KeyboardButton(text="/help"),
            KeyboardButton(text="/my_id"),
        ],
    ]

    if is_owner(user_id):
        rows.insert(
            4,
            [
                KeyboardButton(text="➕ Добавить админа"),
                KeyboardButton(text="➖ Удалить админа"),
            ],
        )

    rows.append([KeyboardButton(text="/cancel"), KeyboardButton(text="❌ Скрыть панель")])

    return ReplyKeyboardMarkup(
        keyboard=rows,
        resize_keyboard=True,
        is_persistent=True,
        input_field_placeholder="Выберите действие…",
    )


def _parse_user_id(text: str) -> int | None:
    raw = (text or "").strip()
    if raw.lower().startswith("id "):
        raw = raw[3:].strip()
    if not raw.isdigit():
        return None
    value = int(raw)
    return value if value > 0 else None


def _format_ban_text(ban: dict) -> str:
    until = ban.get("banned_until")
    reason = (ban.get("reason") or "").strip()
    when = f"до {until}" if until else "навсегда"
    base = f"⛔ Вам запрещена запись ({when})."
    return base + (f"\nПричина: {reason}" if reason else "")


@dp.message(CommandStart(), F.chat.type == "private")
async def cmd_start(message: Message):
    user_id = message.from_user.id

    if is_admin(user_id):
        await message.answer(
            "Привет. Вы вошли как администратор.\n\n"
            f"{private_help_text(user_id)}"
        )
    else:
        await message.answer(
            "Привет.\n"
            "Это бот записи на тренировку.\n\n"
            "Если вы владелец бота и ещё не знаете свой id, отправьте /my_id,\n"
            "впишите его в config.py как OWNER_ID и перезапустите бота."
        )


@dp.message(Command("help"), F.chat.type == "private")
async def cmd_help(message: Message):
    await message.answer(private_help_text(message.from_user.id))


@dp.message(Command("panel"), F.chat.type == "private")
async def cmd_panel(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("У вас нет прав для этой команды.")
        return
    await message.answer(
        "Панель управления записью (клавиатура снизу):",
        reply_markup=build_admin_panel_reply_kb(message.from_user.id),
    )


@dp.message(Command("hide_panel"), F.chat.type == "private")
async def cmd_hide_panel(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("У вас нет прав для этой команды.")
        return
    await message.answer("Панель скрыта.", reply_markup=ReplyKeyboardRemove())


@dp.message(F.text == "❌ Скрыть панель", F.chat.type == "private")
async def hide_panel_button(message: Message):
    await cmd_hide_panel(message)


@dp.message(F.text == "🗑 Кик (основной)", F.chat.type == "private")
async def panel_kick_active(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer("У вас нет прав.")
        return
    await state.clear()
    await state.update_data(ap_list_name="active")
    await state.set_state(AdminPanelStates.waiting_for_kick_number)
    await message.answer("Введите номер участника (например 3). Можно отменить: /cancel")


@dp.message(F.text == "🗑 Кик (ожидание)", F.chat.type == "private")
async def panel_kick_waiting(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer("У вас нет прав.")
        return
    await state.clear()
    await state.update_data(ap_list_name="waiting")
    await state.set_state(AdminPanelStates.waiting_for_kick_number)
    await message.answer("Введите номер участника (например 3). Можно отменить: /cancel")


@dp.message(F.text == "⛔ Бан", F.chat.type == "private")
async def panel_ban(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer("У вас нет прав.")
        return
    await state.clear()
    await state.set_state(AdminPanelStates.waiting_for_ban_user_id)
    await message.answer("Введите user_id пользователя (числом). Можно отменить: /cancel")


@dp.message(F.text == "✅ Разбан", F.chat.type == "private")
async def panel_unban(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer("У вас нет прав.")
        return
    await state.clear()
    await state.set_state(AdminPanelStates.waiting_for_unban_user_id)
    await message.answer("Введите user_id для разбана (числом). Можно отменить: /cancel")


@dp.message(F.text == "✅ Разрешить перезапись", F.chat.type == "private")
async def panel_allow_reregister(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer("У вас нет прав.")
        return
    await state.clear()
    await state.set_state(AdminPanelStates.waiting_for_allow_reregister_user_id)
    await message.answer("Введите user_id, которому разрешить повторную запись. Можно отменить: /cancel")


@dp.message(F.text == "⛔ Запретить перезапись", F.chat.type == "private")
async def panel_disallow_reregister(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer("У вас нет прав.")
        return
    await state.clear()
    await state.set_state(AdminPanelStates.waiting_for_disallow_reregister_user_id)
    await message.answer("Введите user_id, которому запретить повторную запись. Можно отменить: /cancel")


@dp.message(F.text == "➕ Добавить админа", F.chat.type == "private")
async def panel_add_admin(message: Message, state: FSMContext):
    if not is_owner(message.from_user.id):
        await message.answer("Добавлять админов может только владелец.")
        return
    await state.clear()
    await state.set_state(AdminPanelStates.waiting_for_add_admin_user_id)
    await message.answer("Введите user_id для добавления админа (числом). Можно отменить: /cancel")


@dp.message(F.text == "➖ Удалить админа", F.chat.type == "private")
async def panel_remove_admin(message: Message, state: FSMContext):
    if not is_owner(message.from_user.id):
        await message.answer("Удалять админов может только владелец.")
        return
    await state.clear()
    await state.set_state(AdminPanelStates.waiting_for_remove_admin_user_id)
    await message.answer("Введите user_id для удаления админа (числом). Можно отменить: /cancel")


@dp.callback_query(F.data.startswith("ap:"))
async def admin_panel_callback(callback: CallbackQuery, bot: Bot, state: FSMContext):
    if not callback.from_user or not is_admin(callback.from_user.id):
        await callback.answer("Нет прав.", show_alert=True)
        return

    action = (callback.data or "")[3:]
    training = db.get_open_training()

    if action == "help":
        await callback.answer()
        if callback.message:
            await callback.message.answer(private_help_text(callback.from_user.id))
        return

    if action == "my_id":
        await callback.answer()
        if callback.message:
            await callback.message.answer(f"Ваш user_id: {callback.from_user.id}")
        return

    if action == "admins":
        admins = db.list_admins()
        await callback.answer()
        if not callback.message:
            return
        if not admins:
            await callback.message.answer("Список админов пуст.")
            return
        lines = ["Админы:"]
        for admin in admins:
            lines.append(f"- {admin['user_id']} ({admin['role']})")
        await callback.message.answer("\n".join(lines))
        return

    if action == "bans":
        await callback.answer()
        if not callback.message:
            return
        rows = db.list_bans()
        if not rows:
            await callback.message.answer("Активных банов нет.")
            return
        lines = ["Активные баны:"]
        for ban in rows[:50]:
            until = ban.get("banned_until") or "навсегда"
            reason = (ban.get("reason") or "").strip()
            tail = f" — {reason}" if reason else ""
            lines.append(f"- {ban['user_id']}: {until}{tail}")
        await callback.message.answer("\n".join(lines))
        return

    if action == "add_admin":
        if not is_owner(callback.from_user.id):
            await callback.answer("Только владелец.", show_alert=True)
            return
        await state.clear()
        await state.set_state(AdminPanelStates.waiting_for_add_admin_user_id)
        await callback.answer()
        if callback.message:
            await callback.message.answer("Введите user_id для добавления админа (числом). Можно отменить: /cancel")
        return

    if action == "remove_admin":
        if not is_owner(callback.from_user.id):
            await callback.answer("Только владелец.", show_alert=True)
            return
        await state.clear()
        await state.set_state(AdminPanelStates.waiting_for_remove_admin_user_id)
        await callback.answer()
        if callback.message:
            await callback.message.answer("Введите user_id для удаления админа (числом). Можно отменить: /cancel")
        return

    if action == "new_training":
        if training:
            await callback.answer()
            if callback.message:
                await callback.message.answer(
                    "Сейчас уже есть открытая запись.\n"
                    "Сначала закройте её командой /close_training."
                )
            return
        await state.clear()
        await state.set_state(CreateTrainingStates.waiting_for_date)
        await callback.answer()
        if callback.message:
            await callback.message.answer("Введите дату тренировки в формате ДД.ММ.ГГГГ")
        return

    if action == "close_training":
        if not training:
            await callback.answer()
            if callback.message:
                await callback.message.answer("Сейчас нет открытой записи.")
            return
        await state.clear()
        await state.update_data(training=training)
        await state.set_state(CloseTrainingStates.waiting_for_post_confirm)
        await callback.answer()
        if callback.message:
            await callback.message.answer(
                "Закрыть запись на тренировку?\n\n"
                f"{build_training_brief(training)}\n\n"
                "Опубликовать пост об отмене/закрытии в канал?\n"
                "Ответьте: Да / Нет (или /cancel)"
            )
        return

    if action == "training":
        await callback.answer()
        if not callback.message:
            return
        if not training:
            await callback.message.answer("Сейчас нет открытой тренировки.")
            return
        counts = db.get_counts(training["id"])
        if training.get("publish_at"):
            publish_line = f"План публикации (MSK): {training.get('publish_at')} (вручную)"
        else:
            publish_line = "Публикация: вручную"
        availability_line = (
            "Запись для участников: открыта"
            if users_can_register(training)
            else "Запись для участников: закрыта"
        )
        await callback.message.answer(
            "Текущая открытая тренировка:\n\n"
            f"{build_training_brief(training)}\n\n"
            f"{format_counts(training, counts)}\n\n"
            f"{publish_line}\n"
            f"{availability_line}"
        )
        return

    if action == "edit_training":
        if not training:
            await callback.answer()
            if callback.message:
                await callback.message.answer("Сейчас нет открытой тренировки.")
            return
        await state.clear()
        await state.update_data(training_id=training["id"], old_capacity=training["capacity"])
        await state.set_state(EditTrainingStates.waiting_for_capacity)
        await callback.answer()
        if callback.message:
            await callback.message.answer(
                "Изменение лимита мест.\n\n"
                f"{build_training_brief(training)}\n\n"
                "Введите новый лимит (1–200) или /cancel.\n"
                "Важно: если уменьшить лимит ниже числа участников в основном списке, "
                "часть людей будет перенесена в лист ожидания."
            )
        return

    if action in {"list_active", "list_waiting", "kick_active", "kick_waiting"} and not training:
        await callback.answer()
        if callback.message:
            await callback.message.answer("Сейчас нет открытой тренировки.")
        return

    if action == "list_active":
        active = db.list_registrations(training["id"], "active")
        counts = db.get_counts(training["id"])
        text = (
            "Основной список:\n"
            f"{build_training_brief(training)}\n\n"
            f"{format_counts(training, counts)}\n\n"
            f"{render_registrations('Участники:', active)}"
        )
        await callback.answer()
        if callback.message:
            await callback.message.answer(text)
        return

    if action == "list_waiting":
        waiting = db.list_registrations(training["id"], "waiting")
        await callback.answer()
        if callback.message:
            await callback.message.answer(render_registrations("Лист ожидания:", waiting))
        return

    if action in {"kick_active", "kick_waiting"}:
        await state.clear()
        await state.update_data(ap_list_name="active" if action == "kick_active" else "waiting")
        await state.set_state(AdminPanelStates.waiting_for_kick_number)
        await callback.answer()
        if callback.message:
            await callback.message.answer("Введите номер участника (например 3). Можно отменить: /cancel")
        return

    if action == "ban":
        await state.clear()
        await state.set_state(AdminPanelStates.waiting_for_ban_user_id)
        await callback.answer()
        if callback.message:
            await callback.message.answer("Введите user_id пользователя (числом). Можно отменить: /cancel")
        return

    if action == "unban":
        await state.clear()
        await state.set_state(AdminPanelStates.waiting_for_unban_user_id)
        await callback.answer()
        if callback.message:
            await callback.message.answer("Введите user_id для разбана (числом). Можно отменить: /cancel")
        return

    await callback.answer()


@dp.message(AdminPanelStates.waiting_for_add_admin_user_id, F.chat.type == "private")
async def admin_panel_add_admin(message: Message, state: FSMContext):
    if not is_owner(message.from_user.id):
        await message.answer("Добавлять админов может только владелец.")
        return

    text = normalize_spaces(message.text or "")
    if text.lower() in {"/cancel", "cancel", "отмена", "/отмена"}:
        await state.clear()
        await message.answer("Действие отменено.")
        return

    user_id = _parse_user_id(text)
    if not user_id:
        await message.answer("Введите user_id числом (например 123456789) или /cancel.")
        return

    _, info = db.add_admin(user_id, message.from_user.id)
    await state.clear()
    await message.answer(info)


@dp.message(AdminPanelStates.waiting_for_remove_admin_user_id, F.chat.type == "private")
async def admin_panel_remove_admin(message: Message, state: FSMContext):
    if not is_owner(message.from_user.id):
        await message.answer("Удалять админов может только владелец.")
        return

    text = normalize_spaces(message.text or "")
    if text.lower() in {"/cancel", "cancel", "отмена", "/отмена"}:
        await state.clear()
        await message.answer("Действие отменено.")
        return

    user_id = _parse_user_id(text)
    if not user_id:
        await message.answer("Введите user_id числом (например 123456789) или /cancel.")
        return

    _, info = db.remove_admin(user_id)
    await state.clear()
    await message.answer(info)


@dp.message(AdminPanelStates.waiting_for_kick_number, F.chat.type == "private")
async def admin_panel_kick_number(message: Message, bot: Bot, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer("У вас нет прав.")
        return

    text = normalize_spaces(message.text or "")
    if text.lower() in {"/cancel", "cancel", "отмена", "/отмена"}:
        await state.clear()
        await message.answer("Действие отменено.")
        return

    training = db.get_open_training()
    if not training:
        await state.clear()
        await message.answer("Сейчас нет открытой тренировки.")
        return

    data = await state.get_data()
    list_name = data.get("ap_list_name")
    if list_name not in ("active", "waiting"):
        await state.clear()
        await message.answer("Не удалось определить список. Откройте /panel заново.")
        return

    raw = text.lower()
    if not raw.isdigit():
        await message.answer("Введите номер числом (например 3) или /cancel.")
        return

    queue_number = int(raw)
    if queue_number <= 0 or queue_number > 500:
        await message.answer("Номер выглядит неверно. Введите число (например 3) или /cancel.")
        return

    result = db.admin_kick_by_queue(training["id"], list_name, queue_number, cancelled_by=message.from_user.id)
    await state.clear()
    if not result.get("ok"):
        await message.answer("Не найден участник с таким номером в выбранном списке.")
        return

    removed = result["removed"]
    promoted = result.get("promoted")

    who = removed.get("fio") or f"id {removed.get('user_id')}"
    await message.answer(f"Удалён(а): {who} из {list_name} (№{queue_number}).")

    await notify_admins(
        bot,
        f"🗑 Админ удалил участника из {list_name}:\n{who} (№{queue_number})",
        exclude_user_id=message.from_user.id,
    )

    try:
        removed_place = "основного списка" if list_name == "active" else "листа ожидания"
        await send_to_dm_topic(
            bot,
            removed["dm_chat_id"],
            removed["dm_topic_id"],
            "⛔ Ваша запись на тренировку отменена администратором.\n\n"
            f"{build_training_brief(training)}\n\n"
            f"Вы были удалены из {removed_place} (№{queue_number}).\n"
            "Повторная запись на ЭТУ тренировку недоступна, пока админ не разрешит.",
        )
    except Exception as e:
        logging.warning("Не удалось уведомить удалённого участника: %s", e)

    if promoted:
        try:
            await send_to_dm_topic(
                bot,
                promoted["dm_chat_id"],
                promoted["dm_topic_id"],
                "✅ Для вас освободилось место.\n"
                "Вы переведены из листа ожидания в основной список.\n"
                f"Ваш новый номер: {promoted['queue_number']}",
            )
        except Exception as e:
            logging.warning("Не удалось уведомить promoted user: %s", e)


@dp.message(AdminPanelStates.waiting_for_ban_user_id, F.chat.type == "private")
async def admin_panel_ban_user(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer("У вас нет прав.")
        return

    text = normalize_spaces(message.text or "")
    if text.lower() in {"/cancel", "cancel", "отмена", "/отмена"}:
        await state.clear()
        await message.answer("Действие отменено.")
        return

    user_id = _parse_user_id(text)
    if not user_id:
        await message.answer("Введите user_id числом (например 123456789) или /cancel.")
        return

    await state.update_data(ap_ban_user_id=user_id)
    await state.set_state(AdminPanelStates.waiting_for_ban_days)
    await message.answer("На сколько дней бан? Введите число. 0 = навсегда.")


@dp.message(AdminPanelStates.waiting_for_ban_days, F.chat.type == "private")
async def admin_panel_ban_days(message: Message, state: FSMContext):
    text = normalize_spaces(message.text or "")
    if text.lower() in {"/cancel", "cancel", "отмена", "/отмена"}:
        await state.clear()
        await message.answer("Действие отменено.")
        return

    raw = text
    if not raw.isdigit():
        await message.answer("Введите число дней (например 7). 0 = навсегда. Или /cancel.")
        return

    days = int(raw)
    if days < 0 or days > 3650:
        await message.answer("Слишком большое/маленькое значение. Введите 0..3650.")
        return

    await state.update_data(ap_ban_days=days)
    await state.set_state(AdminPanelStates.waiting_for_ban_reason)
    await message.answer("Причина бана? Можно написать текст или '-' чтобы пропустить.")


@dp.message(AdminPanelStates.waiting_for_ban_reason, F.chat.type == "private")
async def admin_panel_ban_reason(message: Message, bot: Bot, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer("У вас нет прав.")
        return

    text = normalize_spaces(message.text or "")
    if text.lower() in {"/cancel", "cancel", "отмена", "/отмена"}:
        await state.clear()
        await message.answer("Действие отменено.")
        return

    data = await state.get_data()
    user_id = data.get("ap_ban_user_id")
    days = int(data.get("ap_ban_days") or 0)
    reason = text
    if reason in {"-", "—", "нет", "none"}:
        reason = ""

    banned_until = None
    if days > 0:
        banned_until = (datetime.utcnow() + timedelta(days=days)).isoformat(timespec="seconds")

    db.ban_user(user_id=user_id, banned_by=message.from_user.id, banned_until=banned_until, reason=reason or None)
    await state.clear()

    await message.answer(
        "Готово. Пользователь забанен " + (f"на {days} дн." if days > 0 else "навсегда") + "."
    )

    try:
        ban = db.get_active_ban(user_id)
        if ban:
            await bot.send_message(chat_id=user_id, text=_format_ban_text(ban))
    except Exception:
        pass


@dp.message(AdminPanelStates.waiting_for_unban_user_id, F.chat.type == "private")
async def admin_panel_unban_user(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer("У вас нет прав.")
        return

    text = normalize_spaces(message.text or "")
    if text.lower() in {"/cancel", "cancel", "отмена", "/отмена"}:
        await state.clear()
        await message.answer("Действие отменено.")
        return

    user_id = _parse_user_id(text)
    if not user_id:
        await message.answer("Введите user_id числом (например 123456789) или /cancel.")
        return

    ok = db.unban_user(user_id)
    await state.clear()
    await message.answer("Разбан выполнен." if ok else "Активного бана не было.")


@dp.message(Command("ban"), F.chat.type == "private")
async def cmd_ban(message: Message, bot: Bot):
    if not is_admin(message.from_user.id):
        await message.answer("У вас нет прав для этой команды.")
        return

    parts = (message.text or "").split(maxsplit=3)
    if len(parts) < 2:
        await message.answer("Использование: /ban user_id [days] [reason]")
        return

    user_id = _parse_user_id(parts[1])
    if not user_id:
        await message.answer("user_id должен быть числом.")
        return

    days = 0
    reason = ""
    if len(parts) >= 3 and parts[2].isdigit():
        days = int(parts[2])
        if days < 0 or days > 3650:
            await message.answer("days должен быть в диапазоне 0..3650")
            return
        if len(parts) == 4:
            reason = normalize_spaces(parts[3])
    else:
        # days not provided; everything after user_id is reason
        if len(parts) >= 3:
            reason = normalize_spaces(" ".join(parts[2:]))

    banned_until = None
    if days > 0:
        banned_until = (datetime.utcnow() + timedelta(days=days)).isoformat(timespec="seconds")

    db.ban_user(user_id=user_id, banned_by=message.from_user.id, banned_until=banned_until, reason=reason or None)
    await message.answer("Пользователь забанен.")

    try:
        ban = db.get_active_ban(user_id)
        if ban:
            await bot.send_message(chat_id=user_id, text=_format_ban_text(ban))
    except Exception:
        pass


@dp.message(Command("unban"), F.chat.type == "private")
async def cmd_unban(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("У вас нет прав для этой команды.")
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Использование: /unban user_id")
        return

    user_id = _parse_user_id(parts[1])
    if not user_id:
        await message.answer("user_id должен быть числом.")
        return

    ok = db.unban_user(user_id)
    await message.answer("Разбан выполнен." if ok else "Активного бана не было.")


@dp.message(Command("bans"), F.chat.type == "private")
async def cmd_bans(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("У вас нет прав для этой команды.")
        return

    rows = db.list_bans()
    if not rows:
        await message.answer("Активных банов нет.")
        return

    lines = ["Активные баны:"]
    for ban in rows[:50]:
        until = ban.get("banned_until") or "навсегда"
        reason = (ban.get("reason") or "").strip()
        tail = f" — {reason}" if reason else ""
        lines.append(f"- {ban['user_id']}: {until}{tail}")
    await message.answer("\n".join(lines))


@dp.message(Command("my_id"), F.chat.type == "private")
async def cmd_my_id(message: Message):
    await message.answer(f"Ваш user_id: {message.from_user.id}")


@dp.message(Command("cancel"), F.chat.type == "private")
async def cmd_cancel(message: Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state:
        await state.clear()
        await message.answer("Текущее действие отменено.")
    else:
        await message.answer("Нет активного пошагового действия.")


@dp.message(Command("new_training"), F.chat.type == "private")
async def cmd_new_training(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer("У вас нет прав для этой команды.")
        return

    existing = db.get_open_training()
    if existing:
        await message.answer(
            "Сейчас уже есть открытая запись.\n"
            "Сначала закройте её командой /close_training."
        )
        return

    await state.clear()
    await state.set_state(CreateTrainingStates.waiting_for_date)
    await message.answer("Введите дату тренировки в формате ДД.ММ.ГГГГ")


@dp.message(CreateTrainingStates.waiting_for_date, F.chat.type == "private")
async def state_training_date(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer("У вас нет прав.")
        return

    training_date = parse_date(message.text or "")
    if not training_date:
        await message.answer("Неверный формат даты. Пример: 25.04.2026")
        return

    await state.update_data(training_date=training_date)
    await state.set_state(CreateTrainingStates.waiting_for_time)
    await message.answer("Введите время тренировки в формате ЧЧ:ММ")


@dp.message(CreateTrainingStates.waiting_for_time, F.chat.type == "private")
async def state_training_time(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer("У вас нет прав.")
        return

    training_time = parse_time(message.text or "")
    if not training_time:
        await message.answer("Неверный формат времени. Пример: 19:00")
        return

    await state.update_data(training_time=training_time)
    await state.set_state(CreateTrainingStates.waiting_for_capacity)
    await message.answer("Введите лимит мест, например 12")


@dp.message(CreateTrainingStates.waiting_for_capacity, F.chat.type == "private")
async def state_training_capacity(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer("У вас нет прав.")
        return

    capacity = parse_capacity(message.text or "")
    if capacity is None:
        await message.answer("Введите целое число от 1 до 200")
        return

    await state.update_data(capacity=capacity)
    await state.set_state(CreateTrainingStates.waiting_for_level)
    await message.answer("Введите уровень, например: Начинающие")


@dp.message(CreateTrainingStates.waiting_for_level, F.chat.type == "private")
async def state_training_level(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer("У вас нет прав.")
        return

    level = normalize_spaces(message.text or "")
    if not level:
        await message.answer("Уровень не может быть пустым.")
        return

    await state.update_data(level=level)
    await state.set_state(CreateTrainingStates.waiting_for_location)
    await message.answer(
        "Введите место проведения тренировки.\n"
        "Пример: Спортзал МИРЭА, корпус А, зал 2"
    )


@dp.message(CreateTrainingStates.waiting_for_location, F.chat.type == "private")
async def state_training_location(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer("У вас нет прав.")
        return

    location = normalize_spaces(message.text or "")
    if not location:
        await message.answer("Место не может быть пустым. Пример: Спортзал МИРЭА, зал 2")
        return

    await state.update_data(location=location)
    await state.set_state(CreateTrainingStates.waiting_for_publish_time)
    await message.answer(
        "Когда вы планируете опубликовать пост в канале?\n\n"
        "Напишите:\n"
        "• сразу\n"
        "или\n"
        "• ДД.ММ.ГГГГ ЧЧ:ММ (по Москве)\n\n"
        "Пример: 25.04.2026 12:30"
    )


@dp.message(CreateTrainingStates.waiting_for_publish_time, F.chat.type == "private")
async def state_training_publish_time(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer("У вас нет прав.")
        return

    text = normalize_spaces(message.text or "")
    lower = text.lower()

    if lower in ("сразу", "now", "сейчас"):
        await state.update_data(publish_at=None, publish_status="manual")
    else:
        dt = parse_publish_datetime_msk(text)
        if not dt:
            await message.answer(
                "Неверный формат.\n"
                "Напишите 'сразу' или дату-время в формате ДД.ММ.ГГГГ ЧЧ:ММ (по Москве).\n"
                "Пример: 25.04.2026 12:30"
            )
            return

        if dt <= moscow_now():
            await message.answer("Время публикации должно быть в будущем (по Москве).")
            return

        await state.update_data(publish_at=to_iso(dt), publish_status="manual_scheduled")

    data = await state.get_data()

    publish_line = (
        "Публикация: вручную"
        if not data.get("publish_at")
        else f"План публикации (MSK): {data['publish_at']} (вручную)"
    )

    preview = (
        "Проверьте данные:\n\n"
        f"Дата: {data['training_date']}\n"
        f"Время: {data['training_time']}\n"
        f"Место: {data['location']}\n"
        f"Лимит: {data['capacity']}\n"
        f"Уровень: {data['level']}\n"
        f"{publish_line}\n\n"
        "Напишите Да для продолжения или /cancel для отмены."
    )

    await state.set_state(CreateTrainingStates.waiting_for_confirm)
    await message.answer(preview)


@dp.message(CreateTrainingStates.waiting_for_confirm, F.chat.type == "private")
async def state_training_confirm(message: Message, state: FSMContext, bot: Bot):
    if not is_admin(message.from_user.id):
        await message.answer("У вас нет прав.")
        return

    answer = normalize_spaces(message.text or "").lower()
    if answer not in ("да", "yes", "ok", "ок"):
        await message.answer("Напишите Да для продолжения или /cancel для отмены.")
        return

    data = await state.get_data()

    training_id = db.create_training(
        training_date=data["training_date"],
        training_time=data["training_time"],
        capacity=data["capacity"],
        level=data["level"],
        location=data["location"],
        created_by=message.from_user.id,
        publish_at=data.get("publish_at"),
        publish_status=data.get("publish_status") or "published",
    )

    training = db.get_training_by_id(training_id)
    await state.clear()

    publish_note = (
        f"План публикации (MSK): {training['publish_at']}"
        if training.get("publish_at")
        else "Публикация: вручную (когда будете готовы)"
    )

    await message.answer(
        "Запись открыта.\n"
        "Бот не публикует посты в канал автоматически — я отправлю вам готовый текст для публикации.\n\n"
        f"{build_training_brief(training)}\n"
        f"{publish_note}"
    )

    post_text = build_channel_post(training)
    await notify_admins(
        bot,
        "Текст поста для канала (скопируйте и опубликуйте/запланируйте в Telegram):\n\n" + post_text,
        exclude_user_id=None,
    )

    await notify_admins(
        bot,
        "🟢 Открыта новая запись на тренировку.\n\n"
        f"{build_training_brief(training)}",
        exclude_user_id=message.from_user.id,
    )


@dp.message(Command("close_training"), F.chat.type == "private")
async def cmd_close_training(message: Message, bot: Bot, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer("У вас нет прав для этой команды.")
        return

    training = db.get_open_training()
    if not training:
        await message.answer("Сейчас нет открытой записи.")
        return

    await state.clear()
    await state.update_data(training=training)
    await state.set_state(CloseTrainingStates.waiting_for_post_confirm)
    await message.answer(
        "Закрыть запись на тренировку?\n\n"
        f"{build_training_brief(training)}\n\n"
        "Опубликовать пост об отмене/закрытии в канал?\n"
        "Ответьте: Да / Нет (или /cancel)"
    )


@dp.message(CloseTrainingStates.waiting_for_post_confirm, F.chat.type == "private")
async def state_close_training_post_confirm(message: Message, bot: Bot, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer("У вас нет прав.")
        return

    text = normalize_spaces(message.text or "").lower()
    if text in {"/cancel", "cancel", "отмена"}:
        await state.clear()
        await message.answer("Закрытие отменено.")
        return

    if text in {"да", "д", "yes", "y"}:
        should_post = True
    elif text in {"нет", "н", "no", "n"}:
        should_post = False
    else:
        await message.answer("Пожалуйста, ответьте: Да или Нет (или /cancel)")
        return

    data = await state.get_data()
    training = data.get("training")
    if not training:
        await state.clear()
        await message.answer("Не удалось найти тренировку для закрытия. Повторите /close_training.")
        return

    db.close_training(training["id"])

    if should_post:
        close_post_text = build_channel_close_post(training)
        await notify_admins(
            bot,
            "Текст поста о закрытии/отмене для канала (скопируйте и опубликуйте/запланируйте в Telegram):\n\n"
            + close_post_text,
            exclude_user_id=None,
        )

    await state.clear()
    await message.answer(
        "Запись закрыта."
        + (" Текст поста разослан админам." if should_post else "")
    )
    await notify_admins(
        bot,
        "⛔ Запись на тренировку закрыта.\n\n"
        f"{build_training_brief(training)}",
        exclude_user_id=message.from_user.id,
    )


@dp.message(Command("training"), F.chat.type == "private")
async def cmd_training(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("У вас нет прав для этой команды.")
        return

    training = db.get_open_training()
    if not training:
        await message.answer("Сейчас нет открытой тренировки.")
        return

    counts = db.get_counts(training["id"])

    publish_status = training.get("publish_status", "published")
    if training.get("publish_at"):
        publish_line = f"План публикации (MSK): {training.get('publish_at')} (вручную)"
    else:
        publish_line = "Публикация: вручную"

    availability_line = (
        "Запись для участников: открыта"
        if users_can_register(training)
        else "Запись для участников: закрыта"
    )

    await message.answer(
        "Текущая открытая тренировка:\n\n"
        f"{build_training_brief(training)}\n\n"
        f"{format_counts(training, counts)}\n\n"
        f"{publish_line}\n"
        f"{availability_line}"
    )


@dp.message(Command("edit_training"), F.chat.type == "private")
async def cmd_edit_training(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer("У вас нет прав для этой команды.")
        return

    training = db.get_open_training()
    if not training:
        await message.answer("Сейчас нет открытой тренировки.")
        return

    await state.clear()
    await state.update_data(training_id=training["id"], old_capacity=training["capacity"])
    await state.set_state(EditTrainingStates.waiting_for_capacity)

    await message.answer(
        "Изменение лимита мест.\n\n"
        f"{build_training_brief(training)}\n\n"
        "Введите новый лимит (1–200) или /cancel.\n"
        "Важно: если уменьшить лимит ниже числа участников в основном списке, "
        "часть людей будет перенесена в лист ожидания."
    )


@dp.message(EditTrainingStates.waiting_for_capacity, F.chat.type == "private")
async def state_edit_training_capacity(message: Message, state: FSMContext, bot: Bot):
    if not is_admin(message.from_user.id):
        await message.answer("У вас нет прав.")
        return

    new_capacity = parse_capacity(message.text or "")
    if new_capacity is None:
        await message.answer("Введите целое число от 1 до 200 (или /cancel)")
        return

    data = await state.get_data()
    training_id = data.get("training_id")
    old_capacity = int(data.get("old_capacity") or 0)

    training = db.get_open_training()
    if not training or training.get("id") != training_id:
        await state.clear()
        await message.answer("Не удалось найти открытую тренировку. Повторите /edit_training.")
        return

    if new_capacity == old_capacity:
        await state.clear()
        await message.answer("Лимит не изменился.")
        return

    result = db.set_capacity_and_rebalance(training_id=training_id, new_capacity=new_capacity)
    await state.clear()

    if not result.get("ok"):
        await message.answer("Не удалось изменить лимит. Попробуйте ещё раз.")
        return

    # Re-fetch training to reflect updated capacity
    training = db.get_training_by_id(training_id)

    demoted = result.get("demoted") or []
    promoted = result.get("promoted") or []

    await message.answer(
        "Лимит обновлён.\n\n"
        f"Было: {old_capacity}\n"
        f"Стало: {new_capacity}\n\n"
        f"Перенесено в ожидание: {len(demoted)}\n"
        f"Переведено в основной список: {len(promoted)}"
    )

    # Notify affected users in channel DMs
    for item in demoted:
        try:
            await send_to_dm_topic(
                bot,
                item["dm_chat_id"],
                item["dm_topic_id"],
                "⚠️ Изменился лимит мест. Вы перенесены в лист ожидания.\n\n"
                f"{build_training_brief(training)}\n"
                f"Ваш номер в ожидании: {item['queue_number']}\n\n"
                "Если вы передумали — отправьте: Отмена",
            )
        except Exception as e:
            logging.warning("Не удалось уведомить пользователя о переносе в ожидание (id=%s): %s", item.get("user_id"), e)

    for item in promoted:
        try:
            await send_to_dm_topic(
                bot,
                item["dm_chat_id"],
                item["dm_topic_id"],
                "✅ Освободилось место. Вы переведены в основной список.\n\n"
                f"{build_training_brief(training)}\n"
                f"Ваш номер в основном списке: {item['queue_number']}\n\n"
                "Команда: Мой номер",
            )
        except Exception as e:
            logging.warning("Не удалось уведомить пользователя о переводе в основной список (id=%s): %s", item.get("user_id"), e)

    await notify_admins(
        bot,
        "ℹ️ Изменён лимит мест тренировки.\n\n"
        f"{build_training_brief(training)}\n\n"
        f"Было: {old_capacity} → Стало: {new_capacity}\n"
        f"Перенесено в ожидание: {len(demoted)}\n"
        f"Переведено в основной список: {len(promoted)}",
        exclude_user_id=None,
    )


@dp.message(Command("list"), F.chat.type == "private")
async def cmd_list(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("У вас нет прав для этой команды.")
        return

    training = db.get_open_training()
    if not training:
        await message.answer("Сейчас нет открытой тренировки.")
        return

    active = db.list_registrations(training["id"], "active")
    counts = db.get_counts(training["id"])

    text = (
        "Основной список:\n"
        f"{build_training_brief(training)}\n\n"
        f"{format_counts(training, counts)}\n\n"
        f"{render_registrations('Участники:', active)}"
    )
    await message.answer(text)


@dp.message(Command("waiting"), F.chat.type == "private")
async def cmd_waiting(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("У вас нет прав для этой команды.")
        return

    training = db.get_open_training()
    if not training:
        await message.answer("Сейчас нет открытой тренировки.")
        return

    waiting = db.list_registrations(training["id"], "waiting")
    await message.answer(render_registrations("Лист ожидания:", waiting))


@dp.message(Command("kick"), F.chat.type == "private")
async def cmd_kick(message: Message, bot: Bot):
    if not is_admin(message.from_user.id):
        await message.answer("У вас нет прав для этой команды.")
        return

    training = db.get_open_training()
    if not training:
        await message.answer("Сейчас нет открытой тренировки.")
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Использование: /kick a3 или /kick w3")
        return

    raw = normalize_spaces(parts[1]).lower().replace(" ", "")
    if len(raw) < 2 or raw[0] not in ("a", "w") or not raw[1:].isdigit():
        await message.answer("Неверный формат. Использование: /kick a3 или /kick w3")
        return

    list_name = "active" if raw[0] == "a" else "waiting"
    queue_number = int(raw[1:])

    result = db.admin_kick_by_queue(training["id"], list_name, queue_number, cancelled_by=message.from_user.id)
    if not result["ok"]:
        await message.answer("Не найден участник с таким номером в выбранном списке.")
        return

    removed = result["removed"]
    promoted = result.get("promoted")

    who = removed.get("fio") or f"id {removed.get('user_id')}"
    await message.answer(f"Удалён(а): {who} из {list_name} (№{queue_number}).")

    await notify_admins(
        bot,
        f"🗑 Админ удалил участника из {list_name}:\n{who} (№{queue_number})",
        exclude_user_id=message.from_user.id,
    )

    # Уведомим удалённого участника (в его DM-топик с каналом)
    try:
        removed_place = "основного списка" if list_name == "active" else "листа ожидания"
        await send_to_dm_topic(
            bot,
            removed["dm_chat_id"],
            removed["dm_topic_id"],
            "⛔ Ваша запись на тренировку отменена администратором.\n\n"
            f"{build_training_brief(training)}\n\n"
            f"Вы были удалены из {removed_place} (№{queue_number}).",
        )
    except Exception as e:
        logging.warning("Не удалось уведомить удалённого участника: %s", e)


@dp.message(Command("allow_reregister"), F.chat.type == "private")
async def cmd_allow_reregister(message: Message, bot: Bot):
    if not is_admin(message.from_user.id):
        await message.answer("У вас нет прав для этой команды.")
        return

    training = db.get_open_training()
    if not training:
        await message.answer("Сейчас нет открытой тренировки.")
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Использование: /allow_reregister user_id")
        return

    user_id = _parse_user_id(parts[1])
    if not user_id:
        await message.answer("user_id должен быть числом.")
        return

    db.allow_reregister(training_id=training["id"], user_id=user_id, allowed_by=message.from_user.id)
    await message.answer("Готово. Повторная запись разрешена для этого пользователя на текущую тренировку.")

    try:
        await bot.send_message(
            chat_id=user_id,
            text=(
                "✅ Вам разрешили повторную запись на тренировку.\n\n"
                f"{build_training_brief(training)}\n\n"
                "Для записи отправьте в сообщения каналу: Секция ФИО"
            ),
        )
    except Exception:
        pass


@dp.message(Command("disallow_reregister"), F.chat.type == "private")
async def cmd_disallow_reregister(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("У вас нет прав для этой команды.")
        return

    training = db.get_open_training()
    if not training:
        await message.answer("Сейчас нет открытой тренировки.")
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Использование: /disallow_reregister user_id")
        return

    user_id = _parse_user_id(parts[1])
    if not user_id:
        await message.answer("user_id должен быть числом.")
        return

    ok = db.disallow_reregister(training_id=training["id"], user_id=user_id)
    await message.answer(
        "Готово. Повторная запись запрещена." if ok else "Разрешения не было (и так запрещено)."
    )


@dp.message(AdminPanelStates.waiting_for_allow_reregister_user_id, F.chat.type == "private")
async def admin_panel_allow_reregister(message: Message, bot: Bot, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer("У вас нет прав.")
        return

    text = normalize_spaces(message.text or "")
    if text.lower() in {"/cancel", "cancel", "отмена", "/отмена"}:
        await state.clear()
        await message.answer("Действие отменено.")
        return

    training = db.get_open_training()
    if not training:
        await state.clear()
        await message.answer("Сейчас нет открытой тренировки.")
        return

    user_id = _parse_user_id(text)
    if not user_id:
        await message.answer("Введите user_id числом (например 123456789) или /cancel.")
        return

    db.allow_reregister(training_id=training["id"], user_id=user_id, allowed_by=message.from_user.id)
    await state.clear()
    await message.answer("Готово. Повторная запись разрешена для этого пользователя на текущую тренировку.")

    try:
        await bot.send_message(
            chat_id=user_id,
            text=(
                "✅ Вам разрешили повторную запись на тренировку.\n\n"
                f"{build_training_brief(training)}\n\n"
                "Для записи отправьте в сообщения каналу: Секция ФИО"
            ),
        )
    except Exception:
        pass


@dp.message(AdminPanelStates.waiting_for_disallow_reregister_user_id, F.chat.type == "private")
async def admin_panel_disallow_reregister(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer("У вас нет прав.")
        return

    text = normalize_spaces(message.text or "")
    if text.lower() in {"/cancel", "cancel", "отмена", "/отмена"}:
        await state.clear()
        await message.answer("Действие отменено.")
        return

    training = db.get_open_training()
    if not training:
        await state.clear()
        await message.answer("Сейчас нет открытой тренировки.")
        return

    user_id = _parse_user_id(text)
    if not user_id:
        await message.answer("Введите user_id числом (например 123456789) или /cancel.")
        return

    ok = db.disallow_reregister(training_id=training["id"], user_id=user_id)
    await state.clear()
    await message.answer(
        "Готово. Повторная запись запрещена." if ok else "Разрешения не было (и так запрещено)."
    )

    if promoted:
        # уведомим поднятого из ожидания (в его DM-топик)
        try:
            await send_to_dm_topic(
                bot,
                promoted["dm_chat_id"],
                promoted["dm_topic_id"],
                "✅ Для вас освободилось место.\n"
                "Вы переведены из листа ожидания в основной список.\n"
                f"Ваш новый номер: {promoted['queue_number']}",
            )
        except Exception as e:
            logging.warning("Не удалось уведомить promoted user: %s", e)


@dp.message(Command("admins"), F.chat.type == "private")
async def cmd_admins(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("У вас нет прав для этой команды.")
        return

    admins = db.list_admins()
    if not admins:
        await message.answer("Список админов пуст.")
        return

    lines = ["Админы:"]
    for admin in admins:
        lines.append(f"- {admin['user_id']} ({admin['role']})")

    await message.answer("\n".join(lines))


@dp.message(Command("add_admin"), F.chat.type == "private")
async def cmd_add_admin(message: Message):
    if not is_owner(message.from_user.id):
        await message.answer("Добавлять админов может только владелец.")
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Использование: /add_admin 123456789")
        return

    try:
        target_id = int(parts[1].strip())
    except ValueError:
        await message.answer("user_id должен быть числом.")
        return

    _, info = db.add_admin(target_id, message.from_user.id)
    await message.answer(info)


@dp.message(Command("remove_admin"), F.chat.type == "private")
async def cmd_remove_admin(message: Message):
    if not is_owner(message.from_user.id):
        await message.answer("Удалять админов может только владелец.")
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Использование: /remove_admin 123456789")
        return

    try:
        target_id = int(parts[1].strip())
    except ValueError:
        await message.answer("user_id должен быть числом.")
        return

    _, info = db.remove_admin(target_id)
    await message.answer(info)


@dp.message(F.chat.is_direct_messages == True)
async def handle_channel_direct_messages(message: Message, bot: Bot):
    if not message.direct_messages_topic:
        return

    if not message.text:
        await reply_to_channel_dm(
            bot,
            message,
            "Пожалуйста, отправьте текстовое сообщение.\n"
            "Пример: Секция Иванов Иван Иванович",
        )
        return

    # В некоторых режимах Telegram может прислать сообщение без from_user (например, анонимно).
    # Для регистрации нужен user_id, поэтому просим отправить сообщение от своего аккаунта.
    if not message.from_user:
        await reply_to_channel_dm(
            bot,
            message,
            "Не удалось определить отправителя сообщения.\n"
            "Похоже, сообщение отправлено анонимно.\n\n"
            "Для записи отправьте сообщение НЕ анонимно от своего аккаунта:\n"
            "Секция Фамилия Имя Отчество",
        )
        return
    
    text = normalize_spaces(message.text)
    lower = text.lower()

    training = db.get_open_training()

    if lower in ("/start", "старт", "help", "/help"):
        if training and users_can_register(training):
            counts = db.get_counts(training["id"])
            note = ""
            if training.get("publish_status") == "scheduled":
                note = "\n\n" + scheduled_registration_text(training)

            await reply_to_channel_dm(
                bot,
                message,
                "Запись на тренировку открыта.\n\n"
                f"{build_training_brief(training)}\n\n"
                f"{format_counts(training, counts)}\n\n"
                "Для записи отправьте:\n"
                "Секция ФИО"
                f"{note}",
            )
        else:
            await reply_to_channel_dm(
                bot,
                message,
                "Сейчас запись закрыта.\n"
                "Когда она откроется, вы сможете написать:\n"
                "Секция ФИО"
            )
        return

    if lower.startswith("секция"):
        if not training:
            await reply_to_channel_dm(bot, message, "Сейчас запись закрыта.")
            return

        ban = db.get_active_ban(message.from_user.id)
        if ban:
            await reply_to_channel_dm(bot, message, _format_ban_text(ban))
            return

        fio = parse_fio(text)
        if not fio:
            await reply_to_channel_dm(
                bot,
                message,
                "Неверный формат.\n"
                "Напишите так:\n"
                "Секция Иванов Иван Иванович"
            )
            return

        result = db.register_user(
            training_id=training["id"],
            user_id=message.from_user.id,
            username=message.from_user.username,
            full_name=message.from_user.full_name,
            fio=fio,
            dm_chat_id=message.chat.id,
            dm_topic_id=message.direct_messages_topic.topic_id,
        )

        if not result["ok"]:
            if result["reason"] == "cancelled_block":
                cancel_source = result.get("cancel_source")
                if cancel_source == "admin":
                    first_line = "⛔ Администратор отменил вашу запись на эту тренировку."
                else:
                    first_line = "⛔ Ваша запись на эту тренировку уже была отменена."

                text_reply = (
                    f"{first_line}\n"
                    "Повторная запись сейчас запрещена.\n"
                    "Админ может разрешить повторную запись, если нужно."
                )
                await reply_to_channel_dm(bot, message, text_reply)
                return
            if result["reason"] == "already_registered":
                if result["status"] == "active":
                    await reply_to_channel_dm(
                        bot,
                        message,
                        "Вы уже записаны на тренировку.\n\n"
                        f"{build_training_brief(training)}\n"
                        f"Ваш номер: {result['number']}"
                    )
                else:
                    await reply_to_channel_dm(
                        bot,
                        message,
                        "Вы уже в листе ожидания.\n\n"
                        f"{build_training_brief(training)}\n"
                        f"Ваш номер в ожидании: {result['number']}"
                    )
            else:
                await reply_to_channel_dm(
                    bot,
                    message,
                    "Не удалось выполнить запись."
                )
            return

        if result["status"] == "active":
            reply_text = (
                "✅ Вы записаны на тренировку.\n\n"
                f"{build_training_brief(training)}\n"
                f"Ваш номер: {result['number']}"
            )
        else:
            reply_text = (
                "🕒 Основные места закончились.\n"
                "Вы добавлены в лист ожидания.\n\n"
                f"{build_training_brief(training)}\n"
                f"Ваш номер в ожидании: {result['number']}"
            )

        await reply_to_channel_dm(bot, message, reply_text)

        await notify_admins(
            bot,
            "Новая запись через сообщения каналу:\n"
            f"ФИО: {fio}\n"
            f"Статус: {'основной список' if result['status'] == 'active' else 'лист ожидания'}\n"
            f"Номер: {result['number']}",
            exclude_user_id=None,
        )
        return

    if lower in ("отмена", "/отмена", "/cancel"):
        if not training or not users_can_register(training):
            await reply_to_channel_dm(
                bot,
                message,
                "Сейчас нет открытой записи, которую можно отменить."
            )
            return

        result = db.cancel_registration(training["id"], message.from_user.id)
        if not result["ok"]:
            await reply_to_channel_dm(
                bot,
                message,
                "У вас нет активной записи на текущую тренировку."
            )
            return

        await reply_to_channel_dm(
            bot,
            message,
            "Ваша запись отменена."
        )

        promoted = result.get("promoted")
        if promoted:
            await send_to_dm_topic(
                bot,
                promoted["dm_chat_id"],
                promoted["dm_topic_id"],
                "✅ Для вас освободилось место.\n"
                "Вы переведены из листа ожидания в основной список.\n"
                f"Ваш новый номер: {promoted['queue_number']}"
            )

        return

    if lower in ("инфо", "/info"):
        if not training or not users_can_register(training):
            await reply_to_channel_dm(
                bot,
                message,
                "Сейчас запись закрыта."
            )
            return

        counts = db.get_counts(training["id"])
        await reply_to_channel_dm(
            bot,
            message,
            "Информация по тренировке:\n\n"
            f"{build_training_brief(training)}\n\n"
            f"{format_counts(training, counts)}"
        )
        return

    if lower in ("мой номер", "моя запись", "/status", "/my"):
        if not training or not users_can_register(training):
            await reply_to_channel_dm(
                bot,
                message,
                "Сейчас нет открытой тренировки."
            )
            return

        registration = db.get_registration_for_user(training["id"], message.from_user.id)
        if not registration:
            await reply_to_channel_dm(
                bot,
                message,
                "На текущую тренировку вы не записаны."
            )
            return

        if registration["status"] == "active":
            text_reply = (
                "Вы записаны в основной список.\n\n"
                f"{build_training_brief(training)}\n"
                f"Ваш номер: {registration['queue_number']}"
            )
        else:
            text_reply = (
                "Вы находитесь в листе ожидания.\n\n"
                f"{build_training_brief(training)}\n"
                f"Ваш номер в ожидании: {registration['queue_number']}"
            )

        await reply_to_channel_dm(bot, message, text_reply)
        return

# ...existing code...

    if lower in ("команды", "/commands", "help", "/help"):
        await reply_to_channel_dm(
            bot,
            message,
            "Доступные команды:\n"
            "• Секция ФИО — записаться\n"
            "• Инфо — информация о тренировке\n"
            "• Мой номер — статус записи\n"
            "• Отмена — отменить запись"
        )
        return

    # Если сообщение не является триггерной командой — молчим
    return


async def publisher_loop(bot: Bot):
    # Раньше здесь была авто-публикация в канал по расписанию.
    # Сейчас бот посты в канал НЕ публикует (вы публикуете вручную через Telegram).
    while True:
        await asyncio.sleep(3600)


async def main():
    if BOT_TOKEN == "ВСТАВЬ_СЮДА_ТОКЕН_БОТА":
        raise ValueError("Укажи BOT_TOKEN в config.py")

    bot = Bot(BOT_TOKEN)
    # Telegram хранит непрочитанные апдейты, пока бот выключен.
    # При запуске polling забирает их и начинает обрабатывать «старые» сообщения.
    # Если это нежелательно — сбрасываем очередь апдейтов при старте.
    await bot.delete_webhook(drop_pending_updates=True)

    retry_delay = 10
    while True:
        try:
            await dp.start_polling(bot)
            return
        except TelegramNetworkError as e:
            logging.warning("Telegram недоступен (%s). Повтор через %s сек...", e, retry_delay)
            await asyncio.sleep(retry_delay)


if __name__ == "__main__":
    asyncio.run(main())
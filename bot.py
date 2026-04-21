import asyncio
import logging
from datetime import datetime

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message

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
                "/new_training — открыть новую запись (сразу или по расписанию)",
                "/close_training — закрыть текущую запись",
                "/training — показать текущую тренировку",
                "/list — основной список",
                "/waiting — лист ожидания",
                "/edit_training — изменить лимит мест",
                "/kick a3 — удалить №3 из основного списка",
                "/kick w3 — удалить №3 из листа ожидания",
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

    result = db.admin_kick_by_queue(training["id"], list_name, queue_number)
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
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
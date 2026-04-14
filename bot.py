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
    waiting_for_confirm = State()


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
    return (
        f"Дата: {training['training_date']}\n"
        f"Время: {training['training_time']}\n"
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
                "/new_training — открыть новую запись",
                "/close_training — закрыть текущую запись",
                "/training — показать текущую тренировку",
                "/list — основной список",
                "/waiting — лист ожидания",
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
    data = await state.get_data()

    preview = (
        "Проверьте данные:\n\n"
        f"Дата: {data['training_date']}\n"
        f"Время: {data['training_time']}\n"
        f"Лимит: {data['capacity']}\n"
        f"Уровень: {data['level']}\n\n"
        "Напишите Да для публикации или /cancel для отмены."
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
        await message.answer("Напишите Да для публикации или /cancel для отмены.")
        return

    data = await state.get_data()

    training_id = db.create_training(
        training_date=data["training_date"],
        training_time=data["training_time"],
        capacity=data["capacity"],
        level=data["level"],
        created_by=message.from_user.id,
    )

    training = db.get_training_by_id(training_id)

    try:
        sent = await bot.send_message(
            chat_id=CHANNEL_ID,
            text=build_channel_post(training),
        )
    except Exception as e:
        db.delete_training(training_id)
        await state.clear()
        await message.answer(
            "Не удалось опубликовать пост в канал.\n"
            f"Проверь CHANNEL_ID и права бота.\n\nОшибка: {e}"
        )
        return

    db.set_channel_message_id(training_id, sent.message_id)
    await state.clear()

    await message.answer(
        "Запись открыта.\n\n"
        f"{build_training_brief(training)}"
    )

    await notify_admins(
        bot,
        "🟢 Открыта новая запись на тренировку.\n\n"
        f"{build_training_brief(training)}",
        exclude_user_id=None,
    )


@dp.message(Command("close_training"), F.chat.type == "private")
async def cmd_close_training(message: Message, bot: Bot):
    if not is_admin(message.from_user.id):
        await message.answer("У вас нет прав для этой команды.")
        return

    training = db.get_open_training()
    if not training:
        await message.answer("Сейчас нет открытой записи.")
        return

    db.close_training(training["id"])

    try:
        await bot.send_message(
            chat_id=CHANNEL_ID,
            text=build_channel_close_post(training),
        )
    except Exception as e:
        logging.warning("Не удалось отправить пост о закрытии: %s", e)

    await message.answer("Запись закрыта.")
    await notify_admins(
        bot,
        "⛔ Запись на тренировку закрыта.\n\n"
        f"{build_training_brief(training)}",
        exclude_user_id=None,
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
    await message.answer(
        "Текущая открытая тренировка:\n\n"
        f"{build_training_brief(training)}\n\n"
        f"{format_counts(training, counts)}"
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

    success, info = db.add_admin(target_id, message.from_user.id)
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

    success, info = db.remove_admin(target_id)
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

    text = normalize_spaces(message.text)
    lower = text.lower()

    training = db.get_open_training()

    if lower in ("/start", "старт", "help", "/help"):
        if training:
            counts = db.get_counts(training["id"])
            await reply_to_channel_dm(
                bot,
                message,
                "Запись на тренировку открыта.\n\n"
                f"{build_training_brief(training)}\n\n"
                f"{format_counts(training, counts)}\n\n"
                "Для записи отправьте:\n"
                "Секция ФИО",
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
            await reply_to_channel_dm(
                bot,
                message,
                "Сейчас запись закрыта."
            )
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
        if not training:
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

        await notify_admins(
            bot,
            f"Отменена запись: {result['cancelled_fio']}",
            exclude_user_id=None,
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

            await notify_admins(
                bot,
                f"Из листа ожидания переведён участник: {promoted['fio']}\n"
                f"Новый номер: {promoted['queue_number']}",
                exclude_user_id=None,
            )
        return

    if lower in ("инфо", "/info"):
        if not training:
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
        if not training:
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

    await reply_to_channel_dm(
        bot,
        message,
        "Доступные команды:\n"
        "• Секция ФИО — записаться\n"
        "• Инфо — информация о тренировке\n"
        "• Мой номер — статус записи\n"
        "• Отмена — отменить запись"
    )


async def main():
    if BOT_TOKEN == "ВСТАВЬ_СЮДА_ТОКЕН_БОТА":
        raise ValueError("Укажи BOT_TOKEN в config.py")

    bot = Bot(BOT_TOKEN)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
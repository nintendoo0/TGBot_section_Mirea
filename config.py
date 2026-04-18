import os
from dotenv import load_dotenv

# Загружаем переменные окружения из .env (если файл существует)
load_dotenv()


def _get_int(name: str, default: int = 0) -> int:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    try:
        return int(value)
    except ValueError:
        return default


BOT_TOKEN = os.getenv("BOT_TOKEN", "ВСТАВЬ_СЮДА_ТОКЕН_БОТА")

# Можно указать:
# 1) публичный username канала, например "@my_volley_channel"
# 2) или числовой chat_id канала
CHANNEL_ID = os.getenv("CHANNEL_ID", "@volleyballmirea")

# Сначала можешь поставить 0, запустить бота, написать ему /my_id,
# потом вписать сюда свой id и перезапустить.
OWNER_ID = _get_int("OWNER_ID", 0)

DB_PATH = os.getenv("DB_PATH", "volleyball_bot.db")



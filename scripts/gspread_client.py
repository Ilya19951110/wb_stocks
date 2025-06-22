import os
import gspread
from scripts.setup_logger import make_logger
from scripts.telegram_logger import TelegramHandler

logger = make_logger(__name__, use_telegram=True)


def get_gspread_client():

    env_path = os.environ.get("GSPREAD_JSON")

    if env_path:
        clean_path = env_path.strip('"').replace("\\", '/')

        if os.path.exists(clean_path):
            logger.info(f"✅ Используем JSON-файл: 'GSPREAD_JSON'")
            return gspread.service_account(filename=clean_path)

    default_path = 'key.json'

    if os.path.exists(default_path):
        logger.info(f"✅ Используем JSON-файл: 'key.json'")
        return gspread.service_account(filename=default_path)

    logger.error(
        f"⚠️ Не найден service account JSON-файл\nУстановите переменную окружения GSPREAD_JSON или создайте key.json.")
    raise FileExistsError("Service account key not found")

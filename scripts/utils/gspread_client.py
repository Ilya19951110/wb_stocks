from scripts.utils.setup_logger import make_logger
from scripts.utils.telegram_logger import send_tg_message
import gspread
import os


logger = make_logger(__name__, use_telegram=False)


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

    send_tg_message(
        f"⚠️ Не найден service account JSON-файл\nУстановите переменную окружения GSPREAD_JSON или создайте key.json.")
    raise FileExistsError("Service account key not found")

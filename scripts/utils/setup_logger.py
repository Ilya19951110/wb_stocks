from scripts.utils.telegram_logger import TelegramHandler
from dotenv import load_dotenv
import colorlog
import logging
import os

load_dotenv()


def make_logger(name: str, use_telegram: bool = False) -> logging.Logger:

    logger = logging.getLogger(name)

    if not logger.hasHandlers():
        logger.setLevel(logging.DEBUG)

        handler = colorlog.StreamHandler()
        handler.setFormatter(colorlog.ColoredFormatter(
            '%(log_color)s%(asctime)s - %(levelname)s - %(message)s',
            log_colors={
                'DEBUG': 'cyan',
                'INFO': 'green',
                'WARNING': 'bold_yellow',
                'ERROR': 'red',
                'CRITICAL': 'bold_red',
            }
        ))

        logger.addHandler(handler)

        if use_telegram:

            tg_token = os.getenv('TG_TOKEN')
            tg_chat_id = os.getenv('MY_TG_CHAT_ID')

            if tg_token and tg_chat_id:
                tg_handler = TelegramHandler(
                    token=tg_token, chat_id=tg_chat_id)

                tg_handler.setFormatter(logging.Formatter(
                    '%(asctime)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
                ))

                logger.addHandler(tg_handler)

            else:
                logger.warning("❗ TG_TOKEN или TG_CHAT_ID не заданы в .env")

    return logger

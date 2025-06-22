import logging
import requests
import os
from dotenv import load_dotenv
load_dotenv()


class TelegramHandler(logging.Handler):

    def __init__(self, token=None, chat_id=None, level=logging.INFO):
        super().__init__(level)
        self.token = token or os.getenv('TG_TOKEN')
        self.chat_id = chat_id or os.getenv('MY_TG_CHAT_ID')

    def emit(self, record):
        log_entry = self.format(record)

        try:
            requests.post(
                f"https://api.telegram.org/bot{self.token}/sendMessage",
                data={"chat_id": self.chat_id, "text": log_entry[:4096]}
            )
        except Exception as e:
            print(f"❌ Ошибка отправки лога в Telegram: {e}")


def send_tg_message(text: str):
    token = os.getenv('TG_TOKEN')
    chat_id = os.getenv('MY_TG_CHAT_ID')

    if not token or not chat_id:
        return

    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data={"chat_id": chat_id, "text": text[:4096]}
        )

    except Exception as e:
        print(f"❌ Ошибка при отправке сообщения в Telegram: {e}")

from dotenv import load_dotenv
import requests
import logging
import os

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


def send_photo_to_telegram(image_path: str, caption: str = ""):
    """
    Отправка изображения в Telegram-бот.
    Необходимо, чтобы переменные окружения TG_BOT_TOKEN и TG_CHAT_ID были заданы.
    """
    bot_token = os.getenv("TG_TOKEN")
    chat_id = os.getenv("MY_TG_CHAT_ID")

    if not bot_token or not chat_id:
        raise ValueError(
            "TG_BOT_TOKEN или TG_CHAT_ID не заданы в переменных окружения.")

    url = f"https://api.telegram.org/bot{bot_token}/sendPhoto"

    with open(image_path, 'rb') as photo:
        response = requests.post(url, data={
            'chat_id': chat_id,
            'caption': caption
        }, files={
            'photo': photo
        })

    if response.status_code != 200:
        raise Exception(f"Ошибка отправки фото: {response.text}")

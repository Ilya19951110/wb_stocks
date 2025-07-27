from scripts.utils.gspread_client import get_gspread_client
from scripts.utils.config.factory import get_requests_url_wb
from utils.config.factory import sheets_names, tables_names
from gspread_dataframe import set_with_dataframe
from scripts.utils.telegram_logger import send_tg_message
import pandas as pd
import requests
import os


def tariffs_for_boxes(clear_range: list[str] = ['A:H']) -> None:
    """
    📦 Загрузка тарифов WB по коробам в Google Таблицу

    Скрипт обращается к API Wildberries, чтобы получить актуальные данные по тарифам
    на доставку и хранение коробов на складах. Результат сохраняется на соответствующий лист
    Google Таблицы, предварительно очищая указанный диапазон.

    Параметры:
    ----------
    clear_range : list[str], optional
        Диапазон ячеек в Google Sheets, который необходимо очистить перед загрузкой данных.
        По умолчанию очищается диапазон 'A:H'.

    Что делает:
    -----------
    1. Авторизуется через `get_gspread_client()` и открывает нужный лист.
    2. Получает API-ключ из переменной окружения `Rachel` для доступа к WB API.
    3. Делает GET-запрос к endpoint `tariffs_box` и парсит JSON-ответ.
    4. Преобразует данные в `pandas.DataFrame`, переименовывает колонки и приводит числовые значения
       к корректному формату (замена '-' на 0, ',' на '.').
    5. Добавляет поля `dtNextBox` и `dtTillMax` — даты актуальности тарифов.
    6. Очищает указанный диапазон на листе Google Sheets.
    7. Загружает обработанные данные в таблицу.

    Зависимости:
    ------------
    - `get_gspread_client()` — авторизация Google Sheets API
    - `get_requests_url_wb()` — словарь с endpoint'ами WB API
    - `sheets_names()` и `tables_names()` — названия листов и таблиц
    - `set_with_dataframe()` — выгрузка pandas.DataFrame в Google Таблицу
    - `send_tg_message()` — уведомления в Telegram
    - `os.getenv('Rachel')` — токен авторизации для доступа к API WB

    Уведомления:
    ------------
    - Отправляется Telegram-сообщение при запуске и завершении скрипта.
    - Также отправляется уведомление после успешной загрузки данных.

    Автор:
    ------
    Илья, Июль 2025
    """
    gs = get_gspread_client()

    spreadsheet = gs.open(tables_names()['wb_matrix_complete'])
    sheets = spreadsheet.worksheet(sheets_names()['tariffs_box_api'])

    headers = {
        "Authorization": os.getenv('Rachel').strip()
    }

    params = {
        "date": '2025-01-01'
    }

    res = requests.get(url=get_requests_url_wb()[
                       'tariffs_box'], headers=headers, params=params)

    result = res.json()

    box = pd.DataFrame(result['response']['data']['warehouseList'])

    box['dtNextBox'] = result['response']['data']['dtNextBox']
    box['dtTillMax'] = result['response']['data']['dtTillMax']

    box = box.rename(columns={
        'boxDeliveryAndStorageExpr': 'Доставка_и_хранение',
        'boxDeliveryBase': 'Доставка_базовая',
        'boxDeliveryLiter': 'Доставка_за_литр',
        'boxStorageBase': 'Хранение_база_день',
        'boxStorageLiter': 'Хранение_за_литр_день',
        'warehouseName': 'Склад'
    })

    target_col = [col for col in box.columns if col not in box.columns[-3:]]

    box[target_col] = box[target_col].replace('-', 0)

    box[target_col] = box[target_col].replace(',', '.', regex=True)

    box[target_col] = box[target_col].apply(
        pd.to_numeric, errors='coerce').fillna(0)

    sheets.batch_clear(clear_range)
    set_with_dataframe(
        sheets,
        box,
        include_column_header=True,
        include_index=False
    )
    send_tg_message(
        "📦 Тарифы WB по коробам успешно обновлены в Google Таблице")


if __name__ == '__main__':

    send_tg_message("🚀 Запуск скрипта выгрузки тарифов по коробам WB")
    tariffs_for_boxes()

    send_tg_message("✅ Скрипт выгрузки тарифов по коробам WB завершен успешно")

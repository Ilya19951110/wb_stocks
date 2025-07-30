import pandas as pd
from scripts.utils.setup_logger import make_logger
from scripts.utils.config.factory import tables_names, sheets_names
from scripts.utils.gspread_client import get_gspread_client
from gspread_dataframe import set_with_dataframe
from scripts.utils.telegram_logger import send_tg_message

logger = make_logger(__name__, use_telegram=False)


def upload_my_werehouse_df_in_assortment_matrix_full(mywerehouse: pd.DataFrame, clear_range=None,) -> None:
    """
    📦 Выгрузка DataFrame из MyWarehouse в Google Таблицу «Ассортиментная матрица. Полная»

    Функция автоматически выгружает датафрейм с остатками или справочником из MyWarehouse
    в указанный лист Google Sheets. Если листа не существует — он создаётся. Если указан диапазон,
    он будет предварительно очищен перед выгрузкой.

    ────────────────────────────────────────────────────────────────────────────

    📥 Аргументы:
    - mywerehouse (pd.DataFrame):
        Датафрейм с данными, полученными из MyWarehouse API. Должен содержать заголовки.

    - clear_range (list[str] | None):
        Список диапазонов для очистки (по умолчанию: ['A:B']).
        Если не указан, будет использован диапазон A:B.

    ────────────────────────────────────────────────────────────────────────────

    📌 Логика работы:

    1. Проверяет, что переданный DataFrame не пуст.
    2. Подключается к Google Sheets через сервисный аккаунт.
    3. Пытается открыть таблицу «Ассортиментная матрица. Полная».
    4. Проверяет наличие нужного листа:
        - если лист есть → очищает указанный диапазон;
        - если листа нет → создаёт новый.
    5. Загружает данные из DataFrame в лист, начиная с ячейки A1.
    6. Логирует все действия и ошибки, при необходимости отправляет в Telegram.

    ────────────────────────────────────────────────────────────────────────────

    📤 Назначение:
    - Таблица: определяется через `get_assortment_matrix_complete()`
    - Лист: определяется по ключу `api_mywarehouse` в `sheets_names()`

    ────────────────────────────────────────────────────────────────────────────

    🧾 Зависимости:
    - `get_gspread_client()` → авторизация
    - `set_with_dataframe()` → загрузка DataFrame в лист
    - `sheets_names()` → получить имя листа
    - `get_assortment_matrix_complete()` → имя таблицы
    - `send_tg_message()` → Telegram-уведомления
    - `make_logger()` → цветной логгер

    ────────────────────────────────────────────────────────────────────────────

    💡 Особенности:
    - Автоматически создаёт лист, если он отсутствует.
    - Загружает данные без индексов, но с заголовками.
    - Все ошибки логируются и дублируются в Telegram.

    🧑‍💻 Автор: Илья  
    📅 Версия: Июль 2025
    """
    sheet_name = sheets_names()['api_mywarehouse']
    if clear_range is None:
        clear_range = ['A:B']

    if mywerehouse.empty:
        logger.warning('дф пустой выгружать нечего')
        return

    try:
        logger.info('🔌 Подключаюсь к gspread...')
        gs = get_gspread_client()

        logger.info('📄 Получаю название таблицы для загрузки...')
        table_name = tables_names()['wb_matrix_complete']

        logger.info(f'📂 Открываю таблицу: {table_name}')
        spreadsheet = gs.open(table_name)

    except Exception:
        msg = '❌ Ошибка при подключении к Google Sheets'
        logger.exception(msg)
        send_tg_message(msg)
        return

    try:

        work_sheets = [ws.title for ws in spreadsheet.worksheets()]
        logger.info(f'🔎 Проверяю наличие листа: "{sheet_name}"')

        if sheet_name in work_sheets:
            logger.info('📄 Лист найден. Очищаю A:B...')

            worksheet = spreadsheet.worksheet(sheet_name)
            worksheet.batch_clear(clear_range)

        else:
            logger.warning(f'🆕 Лист не найден. Создаю: "{sheet_name}"')
            worksheet = spreadsheet.add_worksheet(
                title=sheet_name,
                rows=mywerehouse.shape[0],
                cols=mywerehouse.shape[1]
            )

    except Exception:
        msg = '❌ Ошибка при обработке листа'
        logger.exception(msg)
        send_tg_message(msg)
        return

    try:
        logger.info('📤 Выгружаю данные в Google Sheets...')
        set_with_dataframe(
            worksheet,
            mywerehouse,
            row=1,
            col=1,
            include_column_header=True,
            include_index=False
        )
        logger.info(
            f'✅ Данные ({mywerehouse.shape[0]} строк) выгружены в '
            f'таблицу: "{table_name}", лист: "{sheet_name}"'
        )

    except Exception:
        msg = '❌ Ошибка при выгрузке данных в таблицу'
        logger.exception(msg)
        send_tg_message(msg)

from scripts.utils.config.factory import get_client_info, sheets_names
from scripts.utils.gspread_client import get_gspread_client
from scripts.utils.telegram_logger import send_tg_message
from scripts.utils.setup_logger import make_logger
from gspread_dataframe import set_with_dataframe

from collections import defaultdict
import pandas as pd


logger = make_logger(__name__, use_telegram=False)


def update_barcode(data: dict[str, tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]],
                   clear_range: list[str] = None) -> None:
    """
    📦 Обновление листов штрихкодов в Google Sheets по группам пользователей

    Функция принимает словарь с данными (в том числе датафреймами штрихкодов по ИП),
    группирует их по таблицам согласно `get_group_map()` и загружает соответствующие штрихкоды
    в лист `api_wb_barcode` каждой Google Таблицы.

    ────────────────────────────────────────────────────────────────────────────

    📥 Аргументы:
    - data (dict[str, tuple[DataFrame, DataFrame, DataFrame]]):
        Словарь с ключами — именами таблиц. Значения — кортежи вида:
        (df_ozon, df_wb, df_barcode). Здесь используется только df_barcode.

    - clear_range (list[str] | None):
        Диапазон ячеек для предварительной очистки в Google Sheets.
        По умолчанию: ['A:C']

    ────────────────────────────────────────────────────────────────────────────

    📌 Этапы выполнения:

    1. Получает карту групп пользователей `get_group_map()`:
        - Определяет, в какую таблицу какие пользователи входят.
    2. Группирует штрихкоды (`df_barcode`) по соответствующим таблицам.
    3. Подключается к Google Sheets через `get_gspread_client()`.
    4. Для каждой целевой таблицы:
        - Открывает лист `api_wb_barcode`
        - Очищает диапазон (по умолчанию A:C)
        - Загружает отфильтрованные штрихкоды.
    5. Отправляет уведомления в Telegram об ошибках и успешном завершении.

    ────────────────────────────────────────────────────────────────────────────

    🧾 Зависимости:
    - `get_gspread_client()` → авторизация в Google Sheets
    - `get_group_map()` → карта распределения пользователей по таблицам
    - `sheets_names()` → получение имени листа для выгрузки
    - `set_with_dataframe()` → выгрузка DataFrame в лист
    - `send_tg_message()` → Telegram-уведомления
    - `make_logger()` → логгер с выводом в консоль и файл

    ────────────────────────────────────────────────────────────────────────────

    💡 Особенности:
    - Объединяет штрихкоды из разных ИП в один DataFrame на каждую таблицу.
    - В случае ошибки в одной из таблиц остальные продолжат обрабатываться.
    - Уведомляет о начале и завершении выгрузки через Telegram.

    🧑‍💻 Автор: Илья  
    📅 Версия: Июль 2025
    """

    send_tg_message(
        "🚀 Запущена функция `update_barcode()` — загрузка штрихкодов по группам в Google Sheets")

    sheet_name = sheets_names()['api_wb_barcode']
    MAP = get_client_info()['group_map']

    grouped_df = defaultdict(pd.DataFrame)

    if clear_range is None:
        clear_range = ['A:C']

    try:
        logger.info('🔌 Подключаюсь к Google Sheets клиенту...')
        gs = get_gspread_client()

        logger.info('✅ Подключение к Google Sheets установлено')
    except Exception:
        msg = "❌ Ошибка при подключении к Google Sheets"
        logger.exception(msg)
        send_tg_message(msg)
        return

    for name, (_, barcode) in data.items():
        for sheet, people in MAP.items():
            if name in people:
                grouped_df[sheet] = pd.concat(
                    [grouped_df[sheet], barcode], ignore_index=True)

    logger.info("📊 Данные баркодов сгруппированы по таблицам")

    # === Загрузка данных в таблицы ===
    for sheet, df_barcode in grouped_df.items():
        try:
            logger.info(f'📂 Открываю таблицу: "{sheet}"')
            spreadsheet = gs.open(sheet)

            logger.info(f'📄 Открываю лист: "{sheet_name}"')
            worksheet = spreadsheet.worksheet(sheet_name)

            logger.info(
                f'🧼 Очищаю диапазон {clear_range} в листе "{sheet_name}"')
            worksheet.batch_clear(clear_range)

            logger.info(
                f'⬆️ Загружаю {len(df_barcode)} строк в лист "{sheet_name}" таблицы "{sheet}"')
            set_with_dataframe(
                worksheet,
                df_barcode,
                col=1,
                row=1,
                include_column_header=True,
                include_index=False
            )

            logger.info(
                f'✅ Данные успешно загружены в "{sheet}" → "{sheet_name}"')
        except Exception:
            msg = f"❌ Ошибка при загрузке данных в таблицу '{sheet}' (лист '{sheet_name}')"
            logger.exception(msg)
            send_tg_message(msg)

    send_tg_message("🏁 Загрузка всех баркодов завершена")

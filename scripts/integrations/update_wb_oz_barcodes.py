from scripts.utils.config.factory import tables_names, sheets_names
from scripts.utils.gspread_client import get_gspread_client
from scripts.utils.telegram_logger import send_tg_message
from scripts.utils.config.factory import sheets_names
from scripts.utils.setup_logger import make_logger
from gspread_dataframe import set_with_dataframe
import pandas as pd


logger = make_logger(__name__, use_telegram=False)


def transfer_wb_barcodes_to_oz_sheet(clear_range=['A:B']) -> None:
    """
    📦 Перенос баркодов из WB в таблицу OZON (Google Sheets)

    Скрипт предназначен для автоматического переноса данных по баркодам из таблицы
    «Ассортиментная матрица. Полная» (WB) в целевой лист таблицы «Ассортиментная матрица OZON».

    Используется для синхронизации справочников штрихкодов между двумя маркетплейсами — WB и OZON.

    ────────────────────────────────────────────────────────────────────────────

    📥 Источник:
    - Таблица: получаемая через `get_assortment_matrix_complete()`
    - Лист: определяется по ключу `group_all_barcodes` из `sheets_names()`
    - Столбцы: только первые два (например, "Артикул" и "Баркод")

    📤 Назначение:
    - Таблица: получаемая через `get_assortment_matrix_complete_OZON()`
    - Лист: определяется по ключу `barcodes_wb_oz` из `sheets_names()`
    - Диапазон очистки: по умолчанию `['A:B']`

    ────────────────────────────────────────────────────────────────────────────

    📌 Основные шаги:

    1. Авторизация в Google Sheets через сервисный аккаунт.
    2. Подключение к таблице WB и чтение данных из нужного листа.
    3. Очистка диапазона в целевом листе OZON.
    4. Загрузка отфильтрованных данных (без индексов).
    5. Логирование действий и отправка статусов в Telegram.

    ────────────────────────────────────────────────────────────────────────────

    📦 Зависимости:
    - `get_gspread_client()` → авторизация через Google API
    - `set_with_dataframe()` → запись DataFrame в лист Google Sheets
    - `sheets_names()` → ключи для определения листов
    - `get_assortment_matrix_complete[_OZON]()` → названия таблиц
    - `send_tg_message()` → уведомления об ошибках/успехе
    - `make_logger()` → логгер с цветной консолью и Telegram-логами

    ────────────────────────────────────────────────────────────────────────────

    💡 Особенности:
    - Данные из WB читаются из всех строк, но сохраняются только первые два столбца.
    - Заголовки колонок сохраняются.
    - В случае ошибки или пустых данных скрипт сообщает в Telegram и завершает выполнение.

    🧑‍💻 Автор: Илья  
    📅 Версия: Июль 2025
    """

    try:
        logger.info("🔌 Подключаюсь к GSpread клиенту...")

        gs = get_gspread_client()

        wsheet = sheets_names()['barcodes_wb_oz']
        table_matrix_oz = tables_names()['oz_matrix_complete']

        logger.info(f"🔑 Подключено к таблице OZ: {table_matrix_oz}")
    except Exception:
        msg = "❌ Ошибка при инициализации GSpread или получении имени таблицы OZ"
        send_tg_message(msg)
        logger.exception(msg)
        return

    def connecting_to_amatrix_wb() -> pd.DataFrame:

        sheet_name = sheets_names()['group_all_barcodes']
        try:
            logger.info(f"📥 Получаю данные из таблицы WB: {sheet_name}")

            table_matrix_wb = tables_names()['wb_matrix_complete']
            spreadsheet = gs.open(table_matrix_wb)

            worksheet = spreadsheet.worksheet(sheet_name)

            barcode = [row[:2] for row in worksheet.get_all_values()]

            df_barcode = pd.DataFrame(barcode[1:], columns=barcode[0])

            logger.info(f"✅ Успешно получено {len(df)} строк из таблицы WB")
            return df_barcode

        except Exception:
            msg = "❌ Ошибка при получении данных из таблицы WB"
            send_tg_message(msg)
            logger.exception(msg)
            return
    try:
        spreadsheet_oz = gs.open(table_matrix_oz)

        wsheet_oz = spreadsheet_oz.worksheet(wsheet)
        logger.info(f"📄 Найден лист назначения: '{wsheet}'")

    except Exception:
        msg = f"❌ Ошибка при открытии таблицы или листа '{wsheet}' в OZ"
        send_tg_message(msg)
        logger.exception(msg)
        return

    try:
        logger.info(f"🧹 Очищаю диапазон {clear_range} в листе '{wsheet}'")
        wsheet_oz.batch_clear(clear_range)

        df = connecting_to_amatrix_wb()

        if df.empty:
            msg = "⚠️ Данные из WB пусты, выгрузка отменена"
            logger.warning(msg)
            send_tg_message(msg)
            return

        set_with_dataframe(
            wsheet_oz,
            df,
            row=1,
            col=1,
            include_column_header=True,
            include_index=False
        )
        logger.info(f"✅ Данные успешно выгружены в '{wsheet}'")
    except Exception:
        msg = f"❌ Ошибка при выгрузке данных в лист '{wsheet}'"
        logger.exception(msg)
        send_tg_message(msg)

    send_tg_message(f"✅ Баркоды WB успешно перенесены в '{wsheet}'")


if __name__ == '__main__':
    send_tg_message("🚀 Запуск скрипта по переносу баркодов WB → OZ")

    transfer_wb_barcodes_to_oz_sheet()

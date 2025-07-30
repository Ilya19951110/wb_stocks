from scripts.utils.gspread_client import get_gspread_client
from gspread_dataframe import set_with_dataframe
from scripts.utils.config.factory import sheets_names, tables_names
from scripts.utils.telegram_logger import send_tg_message
from scripts.utils.setup_logger import make_logger
import pandas as pd


logger = make_logger(__name__, use_telegram=False)


def price_transfer_from_am_in_am_oz() -> None:
    """
    🔄 Перенос прайса из «Ассортиментная матрица. Полная (WB)» в «Ассортиментная матрица OZON»

    Функция автоматизирует передачу содержимого одного листа Google Sheets (`Справочник WB`)
    из таблицы WB в таблицу OZON. Используется при синхронизации данных между маркетплейсами.

    ────────────────────────────────────────────────────────────────────────────

    📥 Источник: 
        - Таблица: "Ассортиментная матрица. Полная"
        - Лист: "Справочник WB" (получается из `sheets_names()['directory_wb']`)

    📤 Приёмник: 
        - Таблица: "Ассортиментная матрица OZON"
        - Лист: такой же ("Справочник WB")

    ────────────────────────────────────────────────────────────────────────────

    📦 Что делает:

    1. Авторизуется в Google Sheets с помощью service account.
    2. Открывает лист с данными из WB.
    3. Считывает все строки и превращает их в pandas DataFrame.
    4. Очищает целевой лист в OZON.
    5. Загружает данные в OZON с сохранением заголовков столбцов.
    6. Отправляет логи в Telegram и в логгер.

    ────────────────────────────────────────────────────────────────────────────

    🧾 Используемые утилиты:
        - `get_gspread_client()` → авторизация в Google Sheets
        - `set_with_dataframe()` → загрузка pandas.DataFrame в лист
        - `sheets_names()` → получить название листа ("Справочник WB")
        - `tables_names()` → получить названия исходной и целевой таблиц
        - `send_tg_message()` → уведомление об ошибках/успехе в Telegram
        - `make_logger()` → логгер с цветной консолью

    ────────────────────────────────────────────────────────────────────────────

    💡 Примечание:
        - Предполагается, что структура обоих листов одинакова.
        - Столбцы не переименовываются, индексы не загружаются.
        - Лист в таблице OZON будет полностью очищен перед вставкой.

    🧑‍💻 Автор: Илья
    📅 Версия: Июль 2025
    """

    gs = get_gspread_client()

    table_matrix_wb = tables_names()['wb_matrix_complete']
    table_matrix_oz = tables_names()['oz_matrix_complete']

    sheet_name = sheets_names()['directory_wb']

    logger.info(f"🚀 Старт функции передачи данных: `{sheet_name}`")
    try:
        logger.info(
            "📄 Открываем таблицы: 'Ассортиментная матрица. Полная' и 'Ассортиментная матрица OZON'")
        transfer_spreadsheet = gs.open(table_matrix_wb)
        transfer_sheet = transfer_spreadsheet.worksheet(sheet_name)

        download_spreadsheet = gs.open(table_matrix_oz)
        download_sheet = download_spreadsheet.worksheet(sheet_name)

    except Exception as e:
        msg = f"❌ Ошибка при открытии таблиц или листов: {e}"
        send_tg_message(msg)
        logger.error(msg)
        return

    try:
        logger.info(f"⬇️ Получаем данные из листа `{sheet_name}`")
        transfer_data = transfer_sheet.get_all_values()

        df_transfer = pd.DataFrame(transfer_data[1:], columns=transfer_data[0])
        logger.info(
            f"✅ Данные считаны: {df_transfer.shape[0]} строк, {df_transfer.shape[1]} столбцов")

    except Exception as e:
        msg = f"❌ Ошибка при получении или преобразовании данных: {e}"
        send_tg_message(msg)
        logger.error(msg)
        return

    try:
        logger.info("🧹 Очищаем целевой лист перед вставкой данных")
        download_sheet.clear()

        logger.info("📤 Выгружаем данные в 'Ассортиментная матрица OZON'")
        set_with_dataframe(
            download_sheet,
            df_transfer,
            col=1,
            row=1,
            include_column_header=True,
            include_index=False
        )

        logger.info(
            f"✅ Данные успешно загружены в `{sheet_name}` в таблице OZON")
    except Exception as e:
        msg = f"❌ Ошибка при выгрузке данных в Google Sheets: {e}"
        send_tg_message(msg)
        logger.error(msg)
        return

    send_tg_message("🏁 Завершено без ошибок ✅")


if __name__ == '__main__':
    send_tg_message(f"🚀 Запуск функции `price_transfer_from_am_in_am_oz`")
    price_transfer_from_am_in_am_oz()

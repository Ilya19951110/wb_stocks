from scripts.utils.gspread_client import get_gspread_client
from gspread_dataframe import set_with_dataframe
from scripts.utils.config.factory import sheets_names, tables_names
from scripts.utils.telegram_logger import send_tg_message
from scripts.utils.prepare_values_df import prepare_values_for_sheets
from scripts.utils.setup_logger import make_logger
import pandas as pd
from gspread.utils import rowcol_to_a1


logger = make_logger(__name__, use_telegram=False)


def price_transfer_from_am_in_am_oz() -> None:
    
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

        row, col = df_transfer.shape
        end_cell = rowcol_to_a1(row+1, col)
        download_sheet.batch_clear([f"A2:{end_cell}"])
        
        logger.info("📤 Выгружаем данные в 'Ассортиментная матрица OZON'")

        download_sheet.update(
                values=prepare_values_for_sheets(df_transfer),
                value_input_option="USER_ENTERED",
                range_name='A2'
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
# py -m scripts.integrations.price_transfer_in_oz

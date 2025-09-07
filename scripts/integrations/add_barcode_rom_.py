
from scripts.utils.config.factory import sheets_names, tables_names
from scripts.utils.prepare_values_df import prepare_values_for_sheets
from scripts.utils.gspread_client import get_gspread_client
from scripts.utils.setup_logger import make_logger
from scripts.integrations.utils.tools import get_data_from_google_sheet
import gspread
import pandas as pd

logger = make_logger(__name__, use_telegram=False)


def add_barcode_from_ful_matrix_in_matrix_in_gsh(spreadshet: gspread.Spreadsheet, df: pd.DataFrame, ws: str):
    try:
        logger.info(f"📑 Открываю лист: **{ws}** в таблице: {spreadshet.title}")
        worksheet = spreadshet.worksheet(ws)
        logger.info(f"✅ Лист '{ws}' успешно найден")

        logger.info(f"🧹 Очищаю содержимое листа '{ws}'")
        worksheet.clear()

    except Exception as e:
        logger.warning(f"⚠️ Лист '{ws}' не найден, создаю новый. Ошибка: {e}")
        worksheet = spreadshet.add_worksheet(title=ws, rows=1, cols=1)
        logger.info(f"📄 Новый лист '{ws}' успешно создан")

    try:
        logger.info(
            f"⬆️ Начинаю загрузку DataFrame в лист '{ws}' ({len(df)} строк, {len(df.columns)} колонок)")
        worksheet.update(
            [df.columns.tolist()] + prepare_values_for_sheets(df),
            value_input_option="USER_ENTERED"
        )
        logger.info(f"✅ Данные успешно загружены в лист '{ws}'")

    except Exception as e:
        logger.exception(f"❌ Ошибка при обновлении данных в листе '{ws}': {e}")


if __name__ == '__main__':
    gs = get_gspread_client()

    info_table = tables_names()

    worksheet = sheets_names()['group_all_barcodes']
    extract_table = info_table['wb_matrix_complete']
    add_table = info_table['oz_matrix_complete']

    extract_table_spreadsheet = gs.open(extract_table)
    add_table_spreadsheet = gs.open(add_table)

    finger_puls_azarya_spreadsheet = gs.open('РНП Азарья')
    df = get_data_from_google_sheet(extract_table_spreadsheet, worksheet)

    add_barcode_from_ful_matrix_in_matrix_in_gsh(
        add_table_spreadsheet, df, worksheet
    )

    # выгрузка в рнп азарью
    add_barcode_from_ful_matrix_in_matrix_in_gsh(
        finger_puls_azarya_spreadsheet, df, 'API WB barcode'
    )
# py -m scripts.integrations.add_barcode_rom_

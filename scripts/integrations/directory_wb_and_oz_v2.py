from scripts.integrations.utils.tools import get_data_from_google_sheet
from scripts.utils.gspread_client import get_gspread_client
import gspread
from scripts.utils.prepare_values_df import prepare_values_for_sheets
from scripts.utils.config.factory import sheets_names, tables_names
import pandas as pd
from scripts.utils.setup_logger import make_logger
logger = make_logger(__name__, use_telegram=False)

info_table = tables_names()
info_sheet = sheets_names()


def filtered_data_from_google_sheets(df: pd.DataFrame, filter_data: str):

    if 'ИП' not in df.columns.tolist():
        logger.warning('нету')

    df = df[df['ИП'] == filter_data]

    logger.info('отфильтровано n строк ')
    return df


def add_data_from_google_sheets(spreadsheet: gspread.Spreadsheet, ws: str, df: pd.DataFrame):
    try:
        logger.info(f"📑 Открываю лист '{ws}' в таблице '{spreadsheet.title}'")
        worksheet = spreadsheet.worksheet(ws)

        logger.info(f"🧹 Очищаю лист '{ws}' перед загрузкой данных")
        worksheet.clear()
    except Exception as e:
        logger.warning(f"⚠️ Лист '{ws}' не найден, создаю новый. Ошибка: {e}")
        worksheet = spreadsheet.add_worksheet(title=ws, cols=1, rows=1)

    try:
        logger.info(
            f"⬆️ Загружаю DataFrame в лист '{ws}' "
            f"({len(df)} строк, {len(df.columns)} столбцов)"
        )

        worksheet.update(
            [df.columns.tolist()] + prepare_values_for_sheets(df),
            value_input_option='USER_ENTERED'
        )

        logger.info(f"✅ Данные успешно добавлены в лист '{ws}'")

    except Exception as e:
        logger.exception(f"❌ Ошибка при обновлении листа '{ws}': {e}")


if __name__ == '__main__':
    # РНП Азарья
    Azarya_filter_data = 'Азарья'
    Rachel_filter_data = 'Рахель'
    gs = get_gspread_client()

    Rachel_download_table = 'РНП Рахель'
    Azarya_download_table = 'РНП Азарья'
    WB_MATRIX_SPREADSHEET = gs.open(info_table['wb_matrix_complete'])
    all_directory_wb = get_data_from_google_sheet(
        WB_MATRIX_SPREADSHEET, info_sheet['directory_wb'])

    AZARYA_SPREADSHEET = gs.open(Azarya_download_table)
    RACHEL_SPREADSHEET = gs.open(Rachel_download_table)

    add_data_from_google_sheets(
        AZARYA_SPREADSHEET,
        info_sheet['directory_wb'],
        filtered_data_from_google_sheets(all_directory_wb, Azarya_filter_data),
    )

    add_data_from_google_sheets(
        RACHEL_SPREADSHEET,
        info_sheet['directory_wb'],
        filtered_data_from_google_sheets(all_directory_wb, Rachel_filter_data),
    )


# py -m scripts.integrations.directory_wb_and_oz_v2

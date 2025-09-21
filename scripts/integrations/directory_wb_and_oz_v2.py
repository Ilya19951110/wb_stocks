from scripts.integrations.utils.tools import get_data_from_google_sheet
from scripts.utils.gspread_client import get_gspread_client
import gspread
from scripts.utils.prepare_values_df import prepare_values_for_sheets
from scripts.utils.config.factory import sheets_names, tables_names
import pandas as pd
from scripts.utils.setup_logger import make_logger
logger = make_logger(__name__, use_telegram=False)



def filtered_data_from_google_sheets(df: pd.DataFrame, filter_data: str):

    logger.debug(f"{filter_data}_df до фильтрации по ип {df.shape}".upper())
    if 'ИП' not in df.columns.tolist():
        logger.warning('нету')

    df = df[df['ИП'] == filter_data]

    logger.debug(f"{filter_data}_df после фильтрации по ип {df.shape}".upper())
    return df


def add_data_from_google_sheets(spreadsheet: gspread.Spreadsheet, ws: str, df: pd.DataFrame):
    try:
        logger.info(f"📑 Открываю лист '{ws}' в таблице '{spreadsheet.title}'")
        worksheet = spreadsheet.worksheet(ws)

        logger.info(f"🧹 Очищаю лист '{ws}' перед загрузкой данных")
        row, col = df.shape
        worksheet.batch_clear([f"A2:{chr(64+col)}{row+1}"])

    except Exception as e:
        logger.warning(f"⚠️ Лист '{ws}' не найден, создаю новый. Ошибка: {e}")
        worksheet = spreadsheet.add_worksheet(title=ws, cols=1, rows=1)

    try:
        logger.info(
            f"⬆️ Загружаю DataFrame в лист '{ws}' "
            f"({len(df)} строк, {len(df.columns)} столбцов)"
        )

        worksheet.update(
            values=prepare_values_for_sheets(df),
            value_input_option='USER_ENTERED',
            range_name='A2'
        )

        logger.info(f"✅ Данные успешно добавлены в лист '{ws}'")

    except Exception as e:
        logger.exception(f"❌ Ошибка при обновлении листа '{ws}': {e}")


if __name__ == '__main__':
    info_table = tables_names()
    info_sheet = sheets_names()
    gs = get_gspread_client()

    WB_MATRIX_SPREADSHEET = gs.open(info_table['wb_matrix_complete'])
    directory_wb = info_sheet['directory_wb']

    info = {
        'РНП Рахель':{
            'spreadsheet': gs.open('РНП Рахель'),
            'filtered_data': 'Рахель',
            },
          'РНП Азарья':{
            'spreadsheet': gs.open('РНП Азарья'),
            'filtered_data': 'Азарья'
        }
    }
    
    all_directory_wb = get_data_from_google_sheet(
        WB_MATRIX_SPREADSHEET, directory_wb)
    
    
    # art = all_directory_wb[all_directory_wb['Артикул WB'] == 327998350]

    df = filtered_data_from_google_sheets(
        all_directory_wb, 'Рахель'
    )
  
   
    
    # for table, conf in info.items():

    #     if conf['filtered_data'] not in ():
    #         continue

    #     df = filtered_data_from_google_sheets(
    #         all_directory_wb, conf['filtered_data']
    #     )

    #     add_data_from_google_sheets(
    #         conf['spreadsheet'],
    #         directory_wb,
    #         df,
    #     )
    
   




# py -m scripts.integrations.directory_wb_and_oz_v2

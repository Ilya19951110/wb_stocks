
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

        row, col = df.shape

        worksheet.batch_clear([f"A2:{chr(64+col)}{row+1}"])

    except Exception as e:
        logger.warning(f"⚠️ Лист '{ws}' не найден, создаю новый. Ошибка: {e}")
        worksheet = spreadshet.add_worksheet(title=ws, rows=1, cols=1)
        logger.info(f"📄 Новый лист '{ws}' успешно создан")

    try:
        logger.info(
            f"⬆️ Начинаю загрузку DataFrame в лист '{ws}' ({len(df)} строк, {len(df.columns)} колонок)")
        worksheet.update(
            values=prepare_values_for_sheets(df),
            value_input_option="USER_ENTERED",
            range_name="A2"
        )
        logger.info(f"✅ Данные успешно загружены в лист '{ws}'")

    except Exception as e:
        logger.exception(f"❌ Ошибка при обновлении данных в листе '{ws}': {e}")


if __name__ == '__main__':
    gs = get_gspread_client()
    
    info_table = tables_names()

   
    # возвращаем дф из асортиментной матрицы полной 
    worksheet = sheets_names()['group_all_barcodes']
    df =  get_data_from_google_sheet(gs.open(info_table['wb_matrix_complete']), worksheet),

    info = {
        'ОЗОН':{
            'spreadsheet': gs.open(info_table['oz_matrix_complete']),
            'ws': worksheet
        },  
        'Рахель':{
            'spreadsheet':  gs.open('РНП Рахель'),
            'ws':'API WB barcode',
        },
        'Азарья':{
            'spreadsheet': gs.open('РНП Азарья'),
            'ws':'API WB barcode',
        },
    }
    
    logger.info("🚀 Начинаем загрузку баркодов во все таблицы...")

    for name, conf in info.items():
        logger.info(f"📂 [{name}] Начинаю обновление листа: {conf['ws']}")

        try:
            add_barcode_from_ful_matrix_in_matrix_in_gsh(
                conf['spreadsheet'], df, conf['ws']
            )
            logger.info(f"✅ [{name}] Данные успешно обновлены в листе '{conf['ws']}'")

        except Exception as e:
            logger.exception(f"❌ [{name}] Ошибка при обновлении: {e}")

    logger.info("🎉 Все обновления завершены!")
    
    
# py -m scripts.integrations.add_barcode_rom_

from scripts.utils.gspread_client import get_gspread_client
from scripts.utils.setup_logger import make_logger
from scripts.utils.config.factory import sheets_names, tables_names
from gspread.exceptions import WorksheetNotFound, APIError
from scripts.utils.telegram_logger import send_tg_message
import pandas as pd

logger = make_logger(__name__, use_telegram=False)


def get_block_nmId() -> pd.DataFrame:

    gs = get_gspread_client()
    sheet_name = sheets_names()['block_nmid']
    table_name = tables_names()['wb_matrix_complete']
    try:
        logger.info('🔌 Устанавливаю соединение с Google Sheets API...')

        logger.info(f"📄 Открываю таблицу: {table_name}")
        spreadsheet = gs.open(table_name)

        logger.info(f"📑 Открываю лист: '{sheet_name}'...")
        worksheet = spreadsheet.worksheet(sheet_name)

    except (WorksheetNotFound, APIError, Exception) as e:
        msg = (f"❌ Ошибка при подключении к листу {sheet_name}:\n{e}")
        logger.exception(msg)
        send_tg_message(msg)

        return pd.DataFrame

    try:
        logger.info(f"📥 Считываю данные из листа '{sheet_name}'...")

        block = worksheet.get('A:B')
        df = pd.DataFrame(block[1:], columns=block[0])

        logger.info(f'{df.shape}\n{df.head(5)}')
        logger.info(f'✅ Найдено {len(block)} заблокированных NMID')
        return df

    except Exception:
        msg = f"❌ Ошибка при чтении данных из листа {sheet_name}"
        logger.exception(msg)
        send_tg_message(msg)
        return pd.DataFrame

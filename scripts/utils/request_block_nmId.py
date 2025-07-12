from scripts.utils.gspread_client import get_gspread_client
from scripts.utils.setup_logger import make_logger
from scripts.utils.config.factory import sheets_names, get_assortment_matrix_complete
from gspread.exceptions import WorksheetNotFound, APIError

logger = make_logger(__name__, use_telegram=True)


def get_block_nmId() -> set[int]:

    gs = get_gspread_client()
    sheet_name = sheets_names()['block_nmid']
    table_name = get_assortment_matrix_complete()
    try:
        logger.info('🔌 Устанавливаю соединение с Google Sheets API...')

        logger.info(f"📄 Открываю таблицу: {table_name}")
        spreadsheet = gs.open(table_name)

        logger.info(f"📑 Открываю лист: '{sheet_name}'...")
        worksheet = spreadsheet.worksheet(sheet_name)

    except (WorksheetNotFound, APIError, Exception) as e:
        logger.exception(f"❌ Ошибка при подключении к листу {sheet_name}")
        return set()

    try:
        logger.info(f"📥 Считываю данные из листа '{sheet_name}'...")

        block = set([
            int(row[1]) for row in worksheet.get_all_values()[1:]
            if row[0].strip().isdigit() and int(row[0]) == 0
        ])

        logger.info(f'✅ Найдено {len(block)} заблокированных NMID')
        return block

    except Exception:
        logger.exception(f"❌ Ошибка при чтении данных из листа {sheet_name}")
        return set()

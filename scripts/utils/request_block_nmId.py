from scripts.utils.gspread_client import get_gspread_client
from scripts.utils.setup_logger import make_logger
from gspread.exceptions import WorksheetNotFound, APIError

logger = make_logger(__name__, use_telegram=True)


def get_block_nmId() -> set[int]:

    gs = get_gspread_client()

    try:
        logger.info('🔌 Устанавливаю соединение с Google Sheets API...')

        logger.info('📄 Открываю таблицу: "Ассортиментная матрица. Полная"...')
        spreadsheet = gs.open('Ассортиментная матрица. Полная')

        logger.info('📑 Открываю лист: "БЛОК"...')
        worksheet = spreadsheet.worksheet('БЛОК')

    except (WorksheetNotFound, APIError, Exception) as e:
        logger.exception(f'❌ Ошибка при подключении к листу "БЛОК"')
        return set()

    try:
        logger.info('📥 Считываю данные из листа "БЛОК"...')

        block = set([
            int(row[1]) for row in worksheet.get_all_values()[1:]
            if row[0].strip().isdigit() and int(row[0]) == 0
        ])

        logger.info(f'✅ Найдено {len(block)} заблокированных NMID')
        return block

    except Exception:
        logger.exception(f'❌ Ошибка при чтении данных из листа "БЛОК"')
        return set()

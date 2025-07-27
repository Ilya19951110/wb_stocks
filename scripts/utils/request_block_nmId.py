from scripts.utils.gspread_client import get_gspread_client
from scripts.utils.setup_logger import make_logger
from scripts.utils.config.factory import sheets_names, tables_names
from gspread.exceptions import WorksheetNotFound, APIError
from scripts.utils.telegram_logger import send_tg_message
logger = make_logger(__name__, use_telegram=False)


def get_block_nmId() -> set[int]:
    """
    🔒 Получение списка заблокированных артикулов WB (nmId) из Google Таблицы.

    Функция подключается к таблице Google Sheets (используя gspread) и извлекает
    список заблокированных `nmId` (уникальных идентификаторов товаров Wildberries),
    помеченных нулём в первом столбце.

    Используется для фильтрации/исключения карточек из последующей обработки,
    выгрузки остатков и аналитики.

    ─────────────────────────────────────────────────────────────

    📤 Возвращает:
    ---------------
    set[int]
        Множество `nmId` (целые числа), помеченные как заблокированные.

    🔧 Источники данных:
    --------------------
    - Название таблицы берётся из `tables_names()['wb_matrix_complete']`.
    - Название листа — из `sheets_names()['block_nmid']`.

    ⚠️ Обработка ошибок:
    ---------------------
    - Ошибка подключения к таблице/листу → Telegram уведомление и возврат `set()`.
    - Ошибка чтения/преобразования данных → Telegram уведомление и возврат `set()`.

    📄 Пример строки в таблице:
    ---------------------------
    | 0 | 12345678 |
    | 1 | 87654321 |  ← будет проигнорирована
    ↑       ↑
    |       └── nmId (второй столбец)
    └── 0 → пометка на блокировку

    🧠 Автор: Илья  
    🗓 Версия: Июль 2025
    """

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
        msg = f"❌ Ошибка при чтении данных из листа {sheet_name}"
        logger.exception(msg)
        send_tg_message(msg)
        return set()

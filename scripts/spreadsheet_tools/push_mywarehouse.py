import pandas as pd
from scripts.utils.setup_logger import make_logger
from scripts.utils.config.factory import tables_names, sheets_names
from scripts.utils.gspread_client import get_gspread_client
from scripts.utils.prepare_values_df import prepare_values_for_sheets
from scripts.utils.telegram_logger import send_tg_message
from gspread.utils import rowcol_to_a1
logger = make_logger(__name__, use_telegram=False)


def upload_my_werehouse_df_in_assortment_matrix_full(mywerehouse: pd.DataFrame, start_range=None,num_cols=None) -> None:
    
    sheet_name = sheets_names()['api_mywarehouse']
    if mywerehouse.empty:
        logger.warning('дф пустой выгружать нечего')
        return

    try:
        logger.info('🔌 Подключаюсь к gspread...')
        gs = get_gspread_client()

        logger.info('📄 Получаю название таблицы для загрузки...')
        table_name = tables_names()['wb_matrix_complete']

        logger.info(f'📂 Открываю таблицу: {table_name}')
        spreadsheet = gs.open(table_name)
    except Exception:
        msg = '❌ Ошибка при подключении к Google Sheets'
        logger.exception(msg)
        send_tg_message(msg)
        return

    try:
        logger.info(f'🔎 Проверяю наличие листа: "{sheet_name}"')
        worksheet = spreadsheet.worksheet(sheet_name)
        if start_range:
            clear_range = f"{start_range}:{rowcol_to_a1(1, num_cols).rstrip('123456789')}"
            logger.debug(f'📄 Лист найден. диапозон clear_range {clear_range}...')

            worksheet.batch_clear([clear_range])
            values = prepare_values_for_sheets(mywerehouse)
    except Exception:
        logger.exception(f'🆕 Лист не найден. Создаю: "{sheet_name}"')
        worksheet = spreadsheet.add_worksheet(
                title=sheet_name,
                rows=mywerehouse.shape[0],
                cols=mywerehouse.shape[1]
            )
        values = prepare_values_for_sheets(mywerehouse) + mywerehouse.values.tolist()


    try:
        logger.info('📤 Выгружаю данные в Google Sheets...')
        worksheet.update(
            values=values,
            range_name=start_range,
            value_input_option="USER_ENTERED",
        )
        logger.info(
            f'✅ Данные ({mywerehouse.shape[0]} строк) выгружены в\n'
            f'таблицу: "{table_name}", лист: "{sheet_name}"'
        )

    except Exception:
        msg = '❌ Ошибка при выгрузке данных в таблицу'
        logger.exception(msg)
        send_tg_message(msg)

import pandas as pd
from scripts.utils.setup_logger import make_logger
from scripts.utils.config.factory import get_assortment_matrix_complete, sheets_names
from scripts.utils.gspread_client import get_gspread_client
from gspread_dataframe import set_with_dataframe


logger = make_logger(__name__, use_telegram=True)


def upload_my_werehouse_df_in_assortment_matrix_full(mywerehouse: pd.DataFrame, clear_range=None,) -> None:

    sheet_name = sheets_names()['api_mywarehouse']
    if clear_range is None:
        clear_range = ['A:B']

    if mywerehouse.empty:
        logger.warning('дф пустой выгружать нечего')
        return

    try:
        logger.info('🔌 Подключаюсь к gspread...')
        gs = get_gspread_client()

        logger.info('📄 Получаю название таблицы для загрузки...')
        table_name = get_assortment_matrix_complete()

        logger.info(f'📂 Открываю таблицу: {table_name}')
        spreadsheet = gs.open(table_name)

    except Exception:
        logger.exception('❌ Ошибка при подключении к Google Sheets')
        return

    try:

        work_sheets = [ws.title for ws in spreadsheet.worksheets()]
        logger.info(f'🔎 Проверяю наличие листа: "{sheet_name}"')

        if sheet_name in work_sheets:
            logger.info('📄 Лист найден. Очищаю A:B...')

            worksheet = spreadsheet.worksheet(sheet_name)
            worksheet.batch_clear(clear_range)

        else:
            logger.warning(f'🆕 Лист не найден. Создаю: "{sheet_name}"')
            worksheet = spreadsheet.add_worksheet(
                title=sheet_name,
                rows=mywerehouse.shape[0],
                cols=mywerehouse.shape[1]
            )

    except Exception:
        logger.exception('❌ Ошибка при обработке листа')
        return

    try:
        logger.info('📤 Выгружаю данные в Google Sheets...')
        set_with_dataframe(
            worksheet,
            mywerehouse,
            row=1,
            col=1,
            include_column_header=True,
            include_index=False
        )
        logger.info(
            f'✅ Данные ({mywerehouse.shape[0]} строк) выгружены в '
            f'таблицу: "{table_name}", лист: "{sheet_name}"'
        )

    except Exception:
        logger.exception('❌ Ошибка при выгрузке данных в таблицу')

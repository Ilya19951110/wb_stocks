from scripts.utils.request_block_nmId import get_block_nmId
from scripts.utils.setup_logger import make_logger
from scripts.utils.gspread_client import get_gspread_client
from gspread_dataframe import set_with_dataframe
import pandas as pd

logger = make_logger(__name__, use_telegram=True)


def push_concat_all_cabinet_stocks_to_sheets(
    data: list[pd.DataFrame],
    sheet_name: str,
    sh='Ассортиментная матрица. Полная',
    clear_range=None,
    block_nmid=None
):

    try:
        logger.info('🔌 Подключаюсь к Google Sheets клиенту...')

        gs = get_gspread_client()
        logger.info('✅ Успешно подключен к Google Sheets!')

    except Exception:
        logger.exception("❌ Ошибка при подключении к Google Sheets")
        return

    try:
        spreadsheet = gs.open(sh)
        worksheet = spreadsheet.worksheet(sheet_name)

        if block_nmid:
            logger.info(f'block_nmid длина {len(block_nmid)}')
            df_combined = pd.concat(data, ignore_index=True)

            df_combined = df_combined[~df_combined['Артикул WB'].isin(
                block_nmid)]

            logger.info(f"📊 Объединено строк: {len(df_combined)}")

        else:
            df_combined = pd.concat(data, ignore_index=True)
            logger.info(f"📊 Объединено строк: {len(df_combined)}")

        if clear_range:
            worksheet.batch_clear(clear_range)
            logger.info(
                f"🧼 Очищен диапазон {clear_range} в листе '{sheet_name}'")

        else:
            worksheet.clear()
            logger.info(f"🧼 Полностью очищен лист '{sheet_name}'")

        # set_with_dataframe(
        #     worksheet,
        #     df_combined,
        #     row=1,
        #     col=1,
        #     include_column_header=True,
        #     include_index=False
        # )
        worksheet.update(

            values=[df_combined.columns.values.tolist()] +
            df_combined.values.tolist()
        )

        logger.info(f"✅ Данные успешно выгружены в лист '{sheet_name}'")
    except Exception:
        logger.exception(f"❌ Ошибка при загрузке данных в лист '{sheet_name}'")

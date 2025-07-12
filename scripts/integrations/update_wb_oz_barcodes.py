from scripts.utils.config.factory import get_assortment_matrix_complete, get_assortment_matrix_complete_OZON, sheets_names
from scripts.utils.gspread_client import get_gspread_client
import pandas as pd
from gspread_dataframe import set_with_dataframe
from scripts.utils.setup_logger import make_logger
from scripts.utils.telegram_logger import send_tg_message

logger = make_logger(__name__, use_telegram=True)


def transfer_wb_barcodes_to_oz_sheet(clear_range=['A:B']) -> None:

    try:
        logger.info("🔌 Подключаюсь к GSpread клиенту...")

        gs = get_gspread_client()

        wsheet = sheets_names()['barcodes_wb_oz']
        table_matrix_oz = get_assortment_matrix_complete_OZON()

        logger.info(f"🔑 Подключено к таблице OZ: {table_matrix_oz}")
    except Exception:
        logger.exception(
            "❌ Ошибка при инициализации GSpread или получении имени таблицы OZ")
        return

    def connecting_to_amatrix_wb(sheet_name='API 2') -> pd.DataFrame:
        try:
            logger.info(f"📥 Получаю данные из таблицы WB: {sheet_name}")

            table_matrix_wb = get_assortment_matrix_complete()
            spreadsheet = gs.open(table_matrix_wb)

            worksheet = spreadsheet.worksheet(sheet_name)

            barcode = [row[:2] for row in worksheet.get_all_values()]

            df_barcode = pd.DataFrame(barcode[1:], columns=barcode[0])

            logger.info(f"✅ Успешно получено {len(df)} строк из таблицы WB")
            return df_barcode

        except Exception:
            logger.exception("❌ Ошибка при получении данных из таблицы WB")
            return
    try:
        spreadsheet_oz = gs.open(table_matrix_oz)

        wsheet_oz = spreadsheet_oz.worksheet(wsheet)
        logger.info(f"📄 Найден лист назначения: '{wsheet}'")

    except Exception:
        logger.exception(
            f"❌ Ошибка при открытии таблицы или листа '{wsheet}' в OZ")
        return

    try:
        logger.info(f"🧹 Очищаю диапазон {clear_range} в листе '{wsheet}'")
        wsheet_oz.batch_clear(clear_range)

        df = connecting_to_amatrix_wb()

        if df.empty:
            logger.warning("⚠️ Данные из WB пусты, выгрузка отменена")
            return

        set_with_dataframe(
            wsheet_oz,
            df,
            row=1,
            col=1,
            include_column_header=True,
            include_index=False
        )
        logger.info(f"✅ Данные успешно выгружены в '{wsheet}'")
    except Exception:
        logger.exception(f"❌ Ошибка при выгрузке данных в лист '{wsheet}'")

    logger.info(f"✅ Баркоды WB успешно перенесены в '{wsheet}'")


if __name__ == '__main__':
    send_tg_message("🚀 Запуск скрипта по переносу баркодов WB → OZ")

    transfer_wb_barcodes_to_oz_sheet()

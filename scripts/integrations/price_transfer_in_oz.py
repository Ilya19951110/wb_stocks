from scripts.utils.gspread_client import get_gspread_client
from gspread_dataframe import set_with_dataframe
from scripts.utils.setup_logger import make_logger
import pandas as pd


logger = make_logger(__name__, use_telegram=True)


def price_transfer_from_am_in_am_oz(sheet_name='Справочник WB') -> None:
    # Берем данные из листа 'Справочник WB' в таблице 'Ассортиментная матрица. Полная'
    # и переносим их в такой же лист другой таблицы ('OZON')

    logger.info(f"🚀 Старт функции передачи данных: `{sheet_name}`")
    gs = get_gspread_client()

    try:
        logger.info(
            "📄 Открываем таблицы: 'Ассортиментная матрица. Полная' и 'Ассортиментная матрица OZON'")
        transfer_spreadsheet = gs.open('Ассортиментная матрица. Полная')
        transfer_sheet = transfer_spreadsheet.worksheet(sheet_name)

        download_spreadsheet = gs.open('Ассортиментная матрица OZON')
        download_sheet = download_spreadsheet.worksheet(sheet_name)

    except Exception as e:
        logger.error(f"❌ Ошибка при открытии таблиц или листов: {e}")
        return

    try:
        logger.info(f"⬇️ Получаем данные из листа `{sheet_name}`")
        transfer_data = transfer_sheet.get_all_values()

        df_transfer = pd.DataFrame(transfer_data[1:], columns=transfer_data[0])
        logger.info(
            f"✅ Данные считаны: {df_transfer.shape[0]} строк, {df_transfer.shape[1]} столбцов")

    except Exception as e:
        logger.error(f"❌ Ошибка при получении или преобразовании данных: {e}")
        return

    try:
        logger.info("🧹 Очищаем целевой лист перед вставкой данных")
        download_sheet.clear()

        logger.info("📤 Выгружаем данные в 'Ассортиментная матрица OZON'")
        set_with_dataframe(
            download_sheet,
            df_transfer,
            col=1,
            row=1,
            include_column_header=True,
            include_index=False
        )

        logger.info(
            f"✅ Данные успешно загружены в `{sheet_name}` в таблице OZON")
    except Exception as e:
        logger.error(f"❌ Ошибка при выгрузке данных в Google Sheets: {e}")
        return

    logger.info("🏁 Завершено без ошибок ✅")


if __name__ == '__main__':
    price_transfer_from_am_in_am_oz()

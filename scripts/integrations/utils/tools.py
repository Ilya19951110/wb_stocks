from scripts.utils.setup_logger import make_logger
import gspread
import pandas as pd


logger = make_logger(__name__, use_telegram=False)


def get_data_from_google_sheet(spreadshet: gspread.Spreadsheet, ws: str):
    try:

        worksheet = spreadshet.worksheet(ws)
        logger.info(f"✅ Лист '{ws}' успешно найден")

        data = worksheet.get_all_values()
        logger.info(f"📊 Получено {len(data)} строк из листа '{ws}'")

        if not data:
            logger.warning(
                f"⚠️ Лист '{ws}' пуст — возвращаю пустой DataFrame")
            return pd.DataFrame()

        df = pd.DataFrame(data[1:], columns=data[0])
        logger.info(
            f"🧾 DataFrame создан: {df.shape[0]} строк, {df.shape[1]} колонок")

        return df

    except Exception as e:
        logger.exception(f"❌ Ошибка при чтении данных из листа '{ws}': {e}")
        return pd.DataFrame()

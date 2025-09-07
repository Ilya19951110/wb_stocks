from scripts.utils.gspread_client import get_gspread_client
import pandas as pd
from scripts.utils.prepare_values_df import prepare_values_for_sheets

import gspread
from scripts.utils.setup_logger import make_logger

logger = make_logger(__name__, use_telegram=False)


def get_data_from_google_sheet(spreadsheet: gspread.Spreadsheet, worksheet: str):
    try:
        logger.info(
            f"📥 Читаю данные из таблицы: {spreadsheet.title}, лист: {worksheet}")
        ws = spreadsheet.worksheet(worksheet)

        data = ws.get_all_values()
        logger.info(f"✅ Данные получены: {len(data)} строк")

        return pd.DataFrame(data[1:], columns=data[0])
    except Exception as e:
        logger.exception(
            f"❌ Ошибка при чтении Google Sheet {spreadsheet.title}/{worksheet}: {e}")
        return pd.DataFrame()


def concat_plan_and_stock_to_manager(*args):
    logger.info(f"🔄 Объединяю {len(args)} DataFrame в один")
    df = pd.concat([*args], ignore_index=True)
    logger.info(
        f"✅ Объединённый DataFrame: {df.shape[0]} строк, {df.shape[1]} колонок")
    return df


def push_df_in_table(df: pd.DataFrame, spreadsheet: gspread.Spreadsheet, ws: str):

    try:
        logger.info(
            f"📂 Подключаюсь к таблице: {spreadsheet.title}, лист: {ws}")
        worksheet = spreadsheet.worksheet(ws)

        logger.info(f"🧹 Очищен лист: {ws}")
        worksheet.clear()

    except Exception:
        logger.warning(f"⚠️ Лист {ws} не найден, создаю новый")

        worksheet = spreadsheet.add_worksheet(title=ws, rows=1, cols=1,)

    try:
        logger.info(
            f"⬆️ Загружаю DataFrame в лист {ws}: {df.shape[0]} строк, {df.shape[1]} колонок")

        worksheet.update(
            [df.columns.tolist()] + prepare_values_for_sheets(df),

        )

        logger.info(f"✅ Успешно выгружено в {spreadsheet.title}/{ws}")
    except Exception as e:
        logger.warning(
            f"❌ Ошибка выгрузки в Google Sheet {spreadsheet.title}/{ws}: {e}")


if __name__ == '__main__':
    worksheet: str = 'Остатки API'
    gs = get_gspread_client()

    SHELUDKO_TABLE = 'План продаж ИП Шелудько'
    MISHNEVA_TABLE = 'План продаж ИП Мишнева И'
    MANAGER_TABLE = 'Таблица менеджера'
    CONTENT_TASKS = 'ЗАДАЧИ по КОНТЕНТУ и контроль CTR'

    CONTENT_TASKS_SPREADSHEET = gs.open(CONTENT_TASKS)
    SHELUDKO_SPREADSHEET = gs.open(SHELUDKO_TABLE)
    MISHNEVA_SPREADSHEET = gs.open(MISHNEVA_TABLE)
    MANAGER_SPREADSHEET = gs.open(MANAGER_TABLE)

    df = concat_plan_and_stock_to_manager(
        get_data_from_google_sheet(SHELUDKO_SPREADSHEET, worksheet),
        get_data_from_google_sheet(MISHNEVA_SPREADSHEET, worksheet)
    )

    push_df_in_table(
        df,
        MANAGER_SPREADSHEET,
        worksheet
    )

    push_df_in_table(
        df,
        CONTENT_TASKS_SPREADSHEET,
        worksheet
    )


# py -m scripts.integrations.concat_sales_plan_for_stock

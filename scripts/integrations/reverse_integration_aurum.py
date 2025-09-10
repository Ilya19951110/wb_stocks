import pandas as pd
from scripts.utils.prepare_values_df import prepare_values_for_sheets
from scripts.utils.gspread_client import get_gspread_client
from scripts.utils.setup_logger import make_logger
import gspread
import asyncio

logger = make_logger(__name__, use_telegram=False)

sem = asyncio.Semaphore(2)


async def push_df_in_google_sheets_async(*args, **kwargs):
    async with sem:
        return await asyncio.to_thread(push_df_in_google_sheets(*args, **kwargs))


def get_data_from_manager_table(spreadsheet: gspread.Spreadsheet, ws: str) -> pd.DataFrame:

    try:
        logger.info(
            f"📥 Читаю данные из таблицы менеджера: {spreadsheet.title}, лист: {ws}")
        worksheet = spreadsheet.worksheet(ws)

        data = worksheet.get_all_values()

        if not data:
            logger.warning(f"⚠️ Лист {ws} пустой в {spreadsheet.title}")
            return pd.DataFrame()

        logger.info(
            f"✅ Данные получены: {len(data)} строк, {len(data[0])} колонок")

        df = pd.DataFrame(data[1:], columns=data[0])
        df = df.drop(df.columns[0], axis=1)
        return df

    except Exception as e:
        logger.exception(
            f"❌ Ошибка при чтении {spreadsheet.title}/{ws}: {e}")
        return pd.DataFrame()


def push_df_in_google_sheets(spreadsheet: gspread.Spreadsheet, df: pd.DataFrame, ws: str) -> None:

    logger.info(
        f"⬆️ Начинаю выгрузку DataFrame в таблицу: {spreadsheet.title}, лист: {ws}")

    try:
        worksheet = spreadsheet.worksheet(ws)

        logger.info(f"📂 Найден существующий лист {ws}, очищаю содержимое")
        worksheet.clear()
    except Exception:
        logger.warning(
            f"⚠️ Лист {ws} не найден, создаю новый в {spreadsheet.title}")
        worksheet = spreadsheet.add_worksheet(title=ws, rows=1, cols=1)

    try:
        logger.info(
            f"✍️ Записываю DataFrame в {spreadsheet.title}/{ws}: {df.shape[0]} строк, {df.shape[1]} колонок")

        worksheet.update(
            [df.columns.tolist()] + prepare_values_for_sheets(df),
            value_input_option="USER_ENTERED"
        )

        logger.info(f"✅ Успешно выгружено в {spreadsheet.title}/{ws}")
    except Exception as e:
        logger.exception(
            f"❌ Ошибка при выгрузке в Google Sheet {spreadsheet.title}/{ws}: {e}")


if __name__ == '__main__':

    worksheet = 'Aurum'
    update_sheet = 'Аурум'
    gs = get_gspread_client()

    SHELUDKO_TABLE = 'План продаж ИП Шелудько'
    MISHNEVA_TABLE = 'План продаж ИП Мишнева И'
    MANAGER_TABLE = 'Таблица менеджера'

    SHELUDKO_SPREADSHEET = gs.open(SHELUDKO_TABLE)
    MISHNEVA_SPREADSHEET = gs.open(MISHNEVA_TABLE)
    MANAGER_SPREADSHEET = gs.open(MANAGER_TABLE)

    df = get_data_from_manager_table(MANAGER_SPREADSHEET, worksheet)

    push_df_in_google_sheets(
        MISHNEVA_SPREADSHEET, df, update_sheet
    )

    push_df_in_google_sheets(
        SHELUDKO_SPREADSHEET, df, update_sheet
    )
# py -m scripts.integrations.reverse_integration_aurum

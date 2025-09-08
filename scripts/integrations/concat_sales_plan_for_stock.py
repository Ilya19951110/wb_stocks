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
            value_input_option="USER_ENTERED"

        )

        logger.info(f"✅ Успешно выгружено в {spreadsheet.title}/{ws}")
    except Exception as e:
        logger.warning(
            f"❌ Ошибка выгрузки в Google Sheet {spreadsheet.title}/{ws}: {e}")


if __name__ == '__main__':
    worksheet: str = 'Остатки API'
    worksheet_repo_sales = '8.План продаж'
    gs = get_gspread_client()

    Azarya_download_table = 'РНП Азарья'

    CONTENT_TASKS_SPREADSHEET = gs.open('ЗАДАЧИ по КОНТЕНТУ и контроль CTR')
    SHELUDKO_SPREADSHEET = gs.open('План продаж ИП Шелудько')
    MISHNEVA_SPREADSHEET = gs.open('План продаж ИП Мишнева И')
    MANAGER_SPREADSHEET = gs.open('Таблица менеджера')
    Azarya_ram_spreadsheet = gs.open('Фин модель Иосифовы Р А М')

    download_table_spreadsheet = gs.open(Azarya_download_table)

    df_as_ram = get_data_from_google_sheet(
        Azarya_ram_spreadsheet, worksheet_repo_sales
    )

    df_sheludko_and_mishneva = concat_plan_and_stock_to_manager(
        get_data_from_google_sheet(SHELUDKO_SPREADSHEET, worksheet),
        get_data_from_google_sheet(MISHNEVA_SPREADSHEET, worksheet)
    )

    push_df_in_table(
        df_sheludko_and_mishneva,
        MANAGER_SPREADSHEET,
        worksheet
    )

    push_df_in_table(
        df_sheludko_and_mishneva,
        CONTENT_TASKS_SPREADSHEET,
        worksheet
    )

    push_df_in_table(
        df_as_ram, download_table_spreadsheet, worksheet_repo_sales
    )


# py -m scripts.integrations.concat_sales_plan_for_stock

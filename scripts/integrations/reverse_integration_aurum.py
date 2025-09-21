import pandas as pd
from scripts.integrations.utils.tools import get_data_from_google_sheet
from scripts.utils.prepare_values_df import prepare_values_for_sheets
from scripts.utils.gspread_client import get_gspread_client
from scripts.utils.setup_logger import make_logger
from gspread.utils import rowcol_to_a1
import gspread
import asyncio

logger = make_logger(__name__, use_telegram=False)

sem = asyncio.Semaphore(2)


async def push_df_in_google_sheets_async(*args, **kwargs):
    async with sem:
        return await asyncio.to_thread(push_df_in_google_sheets,*args, **kwargs)


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

        row, col = df.shape
        end_cell = rowcol_to_a1(row + 1, col)
        clear_range = f"A2:{end_cell}"

        logger.debug(f"🧹 Очищаю диапазон {clear_range} в листе '{worksheet.title}' ({row} строк, {col} колонок)")
        worksheet.batch_clear([clear_range])
        logger.debug(f"✅ Диапазон {clear_range} успешно очищен в листе '{worksheet.title}'")
    except Exception:
        logger.warning(
            f"⚠️ Лист {ws} не найден, создаю новый в {spreadsheet.title}")
        worksheet = spreadsheet.add_worksheet(title=ws, rows=1, cols=1)

    try:
        logger.info(
            f"✍️ Записываю DataFrame в {spreadsheet.title}/{ws}: {df.shape[0]} строк, {df.shape[1]} колонок")

        worksheet.update(
            values=prepare_values_for_sheets(df),
            value_input_option='USER_ENTERED',
            range_name='A2'
        )

        logger.info(f"✅ Успешно выгружено в {spreadsheet.title}/{ws}")
    except Exception as e:
        logger.exception(
            f"❌ Ошибка при выгрузке в Google Sheet {spreadsheet.title}/{ws}: {e}")


if __name__ == '__main__':

    
    gs = get_gspread_client()

    MANAGER_DF =  get_data_from_manager_table(gs.open('Таблица менеджера'), ws='Aurum')
    
    info = {
        'Шелудько': {
            'spreadsheet': gs.open('План продаж ИП Шелудько'),
            'update_sheet': 'Аурум',
        },
        'Мишнева': {
            'spreadsheet': gs.open('План продаж ИП Мишнева И'),
            'update_sheet': 'Аурум',
        },
    }

    for name, conf in info.items():
        logger.info(f"📂 [{name}] Начинаю загрузку данных в таблицу → {conf['update_sheet']}")

        try:
            push_df_in_google_sheets(
                conf['spreadsheet'],
                MANAGER_DF,
                conf['update_sheet']
            )
            logger.info(f"✅ [{name}] Данные успешно загружены в '{conf['update_sheet']}'")

        except Exception as e:
            logger.error(f"❌ [{name}] Ошибка при загрузке данных: {e}", exc_info=True)

   
# py -m scripts.integrations.reverse_integration_aurum

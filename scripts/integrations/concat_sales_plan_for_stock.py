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
   
    gs = get_gspread_client()

    SHM = {
            'spreadsheet_sh':gs.open('План продаж ИП Шелудько'),
            'spreadsheet_m':gs.open('План продаж ИП Мишнева И'),
            'func':get_data_from_google_sheet,
            'worksheet': 'Остатки API',
            'concat_func':concat_plan_and_stock_to_manager
        }
    
    fin_model_ram = {
            'spreadsheet': gs.open('Фин модель Иосифовы Р А М'),
            'worksheet': 'План продаж',
            'func': get_data_from_google_sheet,
        }
    
    df_shm = SHM['concat_func'](
        SHM['func'](SHM['spreadsheet_sh'], SHM['worksheet']),
        SHM['func'](SHM['spreadsheet_m'], SHM['worksheet']),
    )

  
    
    df_repo_sales = fin_model_ram['func'](
        fin_model_ram['spreadsheet'], fin_model_ram['worksheet']
    )
    info = {
        'Рахель':{
            'spreadsheet': gs.open('РНП Рахель'),
             'worksheet': 'План продаж',
             'func': push_df_in_table,
             'df': df_repo_sales
        },
        'Азарья': {
            'spreadsheet': gs.open('РНП Азарья'),
            'worksheet': 'План продаж',
            'func': push_df_in_table,
            'df': df_repo_sales
        },
        
        'content_tasks_spreadsheet': {
            'spreadsheet':gs.open('ЗАДАЧИ по КОНТЕНТУ и контроль CTR'),
            'worksheet': 'Остатки API',
            'func': push_df_in_table,
            'df': df_shm
        },
       
        'manager_table':{
            'spreadsheet': gs.open('Таблица менеджера'),
            'worksheet': 'Остатки API',
            'func': push_df_in_table,
            'df': df_shm
        }
    }


    for name, conf in info.items():
        try:
            logger.info(f"📂 [{name}] Загружаю данные в лист '{conf['worksheet']}'...")

            conf['func'](conf['df'], conf['spreadsheet'], conf['worksheet'])

            logger.info(f"✅ [{name}] Данные успешно загружены")
        except Exception as e:
            logger.exception(f"❌ [{name}] Ошибка при обновлении: {e}")

logger.info("🎉 Все обновления завершены!")
        
# py -m scripts.integrations.concat_sales_plan_for_stock

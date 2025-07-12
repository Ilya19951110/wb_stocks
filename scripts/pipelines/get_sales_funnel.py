from scripts.spreadsheet_tools.upload_to_gsheet_advert_sales import save_in_gsh
from scripts.postprocessors.group_sales import get_current_week_sales_df
from scripts.utils.config.factory import get_requests_url_wb, sheets_names
from scripts.engine.run_cabinet import execute_run_cabinet
from scripts.utils.telegram_logger import send_tg_message
from scripts.utils.setup_logger import make_logger
from scripts.engine.universal_main import main
from dotenv import load_dotenv
from datetime import datetime, timedelta
from functools import partial
import pandas as pd
import asyncio
import time


load_dotenv()

logger = make_logger(__name__)


async def report_detail(name, api, session):

    stop, page, all_data = 21, 1, []
    url = get_requests_url_wb()
    while True:

        headers = {
            'Authorization': api,
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        params = {

            'timezone': 'Europe/Moscow',
            'period': {

                'begin': (datetime.now()-timedelta(days=7)).strftime('%Y-%m-%d 00:00:00'),
                'end': (datetime.now()-timedelta(days=1)).strftime('%Y-%m-%d 23:59:59')
            },
            'orderBy': {
                'field': 'openCard',
                'mode': 'desc'
            },
            'page': page
        }
        try:
            async with session.post(url['report_detail'], headers=headers, json=params) as result:
                logger.info(f"{name} подключение {result.status}")

                if result.status != 200:
                    logger.error(f"Ошибка запроса: {result.status}")
                    break

                detail = await result.json()
        except Exception as e:
            logger.error(f"❌❌  Произошла ошибка при запросе к {name}: {e}")

        else:
            cards = detail.get('data', {}).get('cards', [])

            if not cards:
                break

            all_data.extend(cards)

            logger.info(
                f'Получено {len(cards)} записей кабинета {name}. Всего: {len(all_data)}')
            if len(cards) < 1000:
                break

            page += 1
            logger.info(f'Спим {stop} сек')

            await asyncio.sleep(stop)

    return all_data


if __name__ == '__main__':

    send_tg_message(
        f"🏁 Скрипт запущен 'report_detail': {datetime.now():%Y-%m-%d %H:%M:%S}")
    begin = time.time()

    data = asyncio.run(main(
        run_funck=partial(execute_run_cabinet,
                          func_name='report_detail'),
        postprocess_func=get_current_week_sales_df,
        cache_name="test_cache.pkl"
    ))

    worksheet = sheets_names()['api_wb_sales']
    save_in_gsh(dict_data=data, worksheet_name=worksheet)

    end = time.time()
    print(f"⏱ Выполнено за {(end - begin)/60:.2f} минут")

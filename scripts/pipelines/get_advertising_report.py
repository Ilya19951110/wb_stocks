from scripts.spreadsheet_tools.upload_to_gsheet_advert_sales import save_in_gsh
from scripts.postprocessors.group_advert import group_advert_and_id
from scripts.utils.config.factory import get_requests_url_wb, sheets_names
from scripts.utils.telegram_logger import send_tg_message
from scripts.engine.run_cabinet import execute_run_cabinet
from scripts.engine.universal_main import main
from scripts.utils.setup_logger import make_logger
from datetime import datetime, timedelta
from functools import partial
import pandas as pd
import aiohttp
import asyncio
import time


logger = make_logger(__name__)


async def campaign_query(api: str, name: str, session: aiohttp.ClientSession) -> pd.DataFrame:
    """
    campaign_query:
        Функция отправляет запрос к api wb и возвращает ответ от сервера:

    Raises:
        adverts: получает ответ о созданных рекламных кампаниях с их 'рк id'. 
        stats: принимает в params id рк и период date_from и date_to. Метод возвращает статистику по рк за заданный пероид 

    Returns:
        _type_: DataFrame, advert_df - возвращает статистику по рк за определенный период преобразованную в DataFrame


    """
    url_count = get_requests_url_wb()

    url_fullstats = get_requests_url_wb()

    camp_data, advert_sp = [], []

    date_from = (datetime.now()-timedelta(days=7)).strftime('%Y-%m-%d')
    date_to = (datetime.now()-timedelta(days=1)).strftime('%Y-%m-%d')

    headers = {'Authorization': api}

    async with session.get(url_count['promotion_count'], headers=headers) as adverts:

        if adverts.status == 204:
            logger.error(
                f"⚠️ ⚠️ Из 'campaign_query': Нет данных (204) для {name}")
            return pd.DataFrame()

        if adverts.status != 200:
            error_text = await adverts.text()

            logger.error(f"⚠️ ⚠️ Ошибка запроса кампаний: {error_text}")
            raise ValueError(error_text)

        if adverts.status == 200:
            camp = await adverts.json()

        for block in camp['adverts']:
            advert_type = block['type']
            advert_count = block['count']
            advert_status = block['status']

            for advert in block['advert_list']:
                advert['type'] = advert_type
                advert['status'] = advert_status
                advert['count'] = advert_count
                advert_sp.append(advert)

        advert_df = pd.DataFrame(advert_sp)

        if not camp.get('adverts'):
            logger.warning(f"⚠️ Нет активных кампаний для {name}")
            return pd.DataFrame()

        logger.info(
            f"✅✅ Получено {len(advert_df)} кампаний для {name}".upper())

    params = [{'id': c, 'interval': {
        'begin': date_from, 'end': date_to}}for c in advert_df['advertId']]

    logger.info(
        f"📥 Загружаю статистику для {len(params)} кампаний — {name}".upper())

    async with session.post(url_fullstats['advert_fullstats'], headers=headers, json=params) as stats:
        if stats.status != 200:
            error_text = await stats.text()
            raise ValueError(f"⚠️⚠️ Ошибка запроса статистики: {error_text}")

        fullstats = await stats.json()

        if not fullstats:
            logger.warning(f"⚠️ Нет статистики для {name}")
            return pd.DataFrame()

        logger.info(
            f"📊 Получена статистика для {name}: {len(fullstats)} записей".upper())

    if fullstats != None:

        for c in fullstats:
            for d in c['days']:
                for a in d['apps']:
                    for nm in a['nm']:
                        nm['appType'] = a['appType']
                        nm['date'] = d['date']
                        nm['advertId'] = c['advertId']
                        camp_data.append(nm)

        camp_df = pd.DataFrame(camp_data)
        camp_df['Кабинет'] = name
        return camp_df
    else:
        logger.warning(f"⚠️ Нет данных статистики для {name}")
        return pd.DataFrame()


if __name__ == '__main__':

    """
        data - сохраняет результат выполнения функции main() которая находится в файле universal_main.py

        main() - принимает два аргумента:
            1. функция run_cabinet_advert, которая выполняет асинхронный запрос, которая находится в файле run_cabinet.py
            2. Функция group_advert_and_id, которая объединяет и группирует результат выполнения run_cabinet_advert. 
                 group_advert_and_id находится в файле advertising_campaign_statistics.py

        save_in_gsh() - выгружает результат main(), который хранится в data. Принимает 2 аргумента:
            1. data - результат main()
            2. wks_name - имя листа для выгрузки
        save_in_gsh() находится в файле load_in_gsh.py


    """

    send_tg_message(
        f"🏁 Скрипт запущен 'campaign_query': {datetime.now():%Y-%m-%d %H:%M:%S}")
    begin = time.time()

    data = asyncio.run(main(
        run_funck=partial(execute_run_cabinet,
                          func_name='campaign_query'),
        postprocess_func=group_advert_and_id,
        cache_name="test_cache.pkl"
    ))

    worksheet = sheets_names()['api_wb_advert']
    save_in_gsh(dict_data=data, worksheet_name=worksheet)

    end = time.time()
    print(f"Время выполнения программы:\n{(end-begin)/60:.2f} минут")

from scripts.spreadsheet_tools.upload_to_gsheet_advert_sales import save_in_gsh
from scripts.utils.telegram_logger import send_tg_message
from scripts.engine.run_cabinet import execute_run_cabinet
from scripts.engine.universal_main import main
from scripts.utils.setup_logger import make_logger
from datetime import datetime, timedelta
from functools import partial
import pandas as pd
import asyncio
import time
import numpy as np

logger = make_logger(__name__)


async def campaign_query(api, name, session):
    """
    campaign_query:
        Функция отправляет запрос к api wb и возвращает ответ от сервера:

    Raises:
        adverts: получает ответ о созданных рекламных кампаниях с их 'рк id'. 
        stats: принимает в params id рк и период date_from и date_to. Метод возвращает статистику по рк за заданный пероид 

    Returns:
        _type_: DataFrame, advert_df - возвращает статистику по рк за определенный период преобразованную в DataFrame


    """

    url_count = 'https://advert-api.wildberries.ru/adv/v1/promotion/count'
    url2 = "https://advert-api.wildberries.ru/adv/v2/fullstats"

    camp_data, advert_sp = [], []

    date_from = (datetime.now()-timedelta(days=7)).strftime('%Y-%m-%d')
    date_to = (datetime.now()-timedelta(days=1)).strftime('%Y-%m-%d')

    headers = {'Authorization': api}

    async with session.get(url_count, headers=headers) as adverts:

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

    async with session.post(url2, headers=headers, json=params) as stats:
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


def group_advert_and_id(camp_df, ID, name):
    """_summary_
    функция объединяет (merge) и группирует (groupby) два датафрейма, после фильтрует их по столбцам

    Args:
        camp_df (_type_): DataFrame рекламной кампании за определенный период

        ID (_type_): DataFrame возвращается из функции get_cards() которая находится в файле test.py, которая возвращает все созданные карточки
                     товаров с idkt (idkt - это id склейки артикулов wb)

        name (_type_): имя кабинета, которое используется для логирования выполнения кода

    Returns:
        _type_: возвращает DataFrame отфильтрованный по столбцам
    """

    ID['updatedAt'] = pd.to_datetime(ID['updatedAt'])

    latest_idkt = (
        ID.sort_values('updatedAt').drop_duplicates(
            subset='Артикул WB', keep='last').reset_index(drop=True)
    )
    camp_df['date'] = pd.to_datetime(camp_df['date']).dt.date

    camp_df['Неделя'] = pd.to_datetime(camp_df['date']).dt.isocalendar().week
    camp_df = camp_df.rename(columns={'sum': 'expenses'})

    camp_df.drop(columns=['date'], inplace=True)

    logger.info(
        f"{name}💰 Сумма ДО merge: {camp_df['expenses'].sum():,.2f} ₽\033[0m\n\033[93m🔍 Строк до merge: {len(camp_df)}")

    camp_df = pd.merge(
        camp_df.rename(columns={'nmId': 'Артикул WB'}),
        latest_idkt.rename(columns={'ID KT': 'ID'}),
        left_on='Артикул WB',
        right_on='Артикул WB',
        how='left'
    )
    # ID KT
    logger.info(
        f"💥 Сумма ПОСЛЕ merge: {camp_df['expenses'].sum():,.2f} ₽\033[0m\n\033[93m🔍 Строк после merge: {len(camp_df)}")

    camp_df['ID'] = pd.to_numeric(
        camp_df['ID'], errors='coerce').fillna(0).astype(int)

    result = camp_df.groupby(['ID', 'Неделя', 'Артикул WB']).agg({
        'views': 'sum',
        'clicks': 'sum',
        'atbs': 'sum',
        'orders': 'sum',
        'shks': 'sum',
        'sum_price': 'sum',
        'expenses': 'sum'
    }).reset_index()

    result = result.drop_duplicates()

    result = result.rename(columns={
        'views': 'Просмотры',
        'clicks': 'Переходы',
        'atbs': 'Добавления в корзину',
        'orders': 'Количество заказов',
        'shks': 'Количество заказанных товаров',
        'sum_price': 'Сумма заказов',
        'expenses': 'Расход,Р'
    })

    result['CTR'] = np.where(
        result['Просмотры'] == 0,
        0,
        (
            result['Переходы'] / result['Просмотры']
        ).round(2)
    )

    result = result.filter(['ID', 'Неделя', 'Расход,Р', 'Артикул WB', 'CTR'])

    logger.info(
        f"🎯 Агрегация по ID выполнена для {name}!\n {result['Расход,Р'].sum():,.2f}"
    )

    return result


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

    save_in_gsh(dict_data=data, worksheet_name='API WB РК')
    end = time.time()
    print(f"Время выполнения программы:\n{(end-begin)/60:.2f} минут")

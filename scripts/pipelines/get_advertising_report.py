from scripts.spreadsheet_tools.upload_to_gsheet_advert_sales import save_in_gsh
from scripts.postprocessors.group_advert import group_advert_and_id
from scripts.utils.config.factory import get_requests_url_wb, sheets_names
from scripts.utils.telegram_logger import send_tg_message
from scripts.engine.run_cabinet import execute_run_cabinet
from scripts.utils.telegram_logger import send_tg_message
from scripts.engine.universal_main import main
from scripts.utils.setup_logger import make_logger
from datetime import datetime, timedelta
from functools import partial
import pandas as pd
import aiohttp
import asyncio
import time
import os
from dotenv import load_dotenv
load_dotenv()

logger = make_logger(__name__, use_telegram=False)


async def campaign_query(api: str, name: str, session: aiohttp.ClientSession) -> pd.DataFrame:
    """
    📢 Получение рекламной статистики WB по активным кампаниям за последнюю неделю.

    Функция `campaign_query` асинхронно обращается к API Wildberries, чтобы получить:
    1. Список всех рекламных кампаний (по кабинетам).
    2. Детальную статистику по дням, приложениям и товарам внутри этих кампаний.

    🚀 Используется в связке с универсальным движком `main()` и функцией `execute_run_cabinet`.

    ────────────────────────────────────────────────────────────

    🔧 Аргументы:
    -------------
    api : str
        Токен авторизации для доступа к API WB (отдельный на каждый кабинет).

    name : str
        Название кабинета, которое добавляется в итоговый DataFrame как метка.

    session : aiohttp.ClientSession
        Асинхронная HTTP-сессия для выполнения запросов.

    📤 Возвращает:
    -------------
    pd.DataFrame
        Таблица с полями:
            - advertId
            - date
            - appType (мобильное приложение / веб)
            - остальные рекламные метрики
            - Кабинет (имя кабинета)

    🗂️ Используемые ключевые URL:
    -----------------------------
    - `promotion_count` — получить список всех активных кампаний.
    - `advert_fullstats` — получить подробную статистику по всем кампаниям.

    📆 Период:
    ---------
    - Текущая неделя: с (сегодня - 7 дней) по (вчера).
    - Формат: YYYY-MM-DD

    📦 Поведение:
    ------------
    - Если нет кампаний: вернёт пустой DataFrame и логгирует предупреждение.
    - Если API вернёт ошибку: сообщение попадёт в Telegram.
    - Данные из `fullstats` собираются по дням, приложениям и `nmId`.

    📌 Пример запуска:
    ------------------
    asyncio.run(
        campaign_query(
            api='Bearer xxxxxx',
            name='Рахель',
            session=aiohttp.ClientSession()
        )
    )

    🧠 Автор: Илья  
    🗓 Версия: Июль 2025
    """
    url_count = get_requests_url_wb()['promotion_count']

    url_fullstats = get_requests_url_wb()['advert_fullstats']

    camp_data = []

    date_from = (datetime.now()-timedelta(days=7)).strftime('%Y-%m-%d')
    date_to = (datetime.now()-timedelta(days=1)).strftime('%Y-%m-%d')

    headers = {'Authorization': api}

    async with session.get(url_count, headers=headers) as adverts:

        if adverts.status == 204:
            msg = f"⚠️ ⚠️ Из 'campaign_query': Нет данных (204) для {name}"
            logger.error(msg)
            send_tg_message(msg)
            return pd.DataFrame()

        if adverts.status != 200:
            error_text = await adverts.text()

            msg = f"⚠️ ⚠️ Ошибка запроса кампаний:\n{error_text}"
            logger.error(msg)
            raise ValueError(msg)

        if adverts.status == 200:
            camp = await adverts.json()

        advert_id = [
            a['advertId']
            for adv in camp['adverts']
            if 'advert_list' in adv
            for a in adv['advert_list']
        ]

        logger.info(
            f"✅✅ Получено {len(advert_id)} кампаний для {name}".upper())

    params = [{'id': c, 'interval': {
        'begin': date_from, 'end': date_to}} for c in advert_id]

    logger.info(
        f"📥 Загружаю статистику для {len(params)} кампаний — {name}".upper())

    async with session.post(url_fullstats, headers=headers, json=params) as stats:
        if stats.status != 200:
            error_text = await stats.text()
            msg = f"⚠️⚠️ Ошибка запроса статистики: {error_text}"
            send_tg_message(msg)
            raise ValueError(msg)

        fullstats = await stats.json()

        if not fullstats:
            msg = f"⚠️ Нет статистики для {name}"
            logger.warning(msg)
            send_tg_message(msg)
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
        send_tg_message(
            f"✅ Рекламная статистика для кабинета '{name}' успешно получена: {len(camp_df)} строк")

        return camp_df

    else:
        msg = f"⚠️ Нет данных статистики для {name}"
        logger.warning(msg)
        send_tg_message(msg)
        return pd.DataFrame()


if __name__ == '__main__':

    send_tg_message(
        f"🏁 Скрипт запущен 'campaign_query': {datetime.now():%Y-%m-%d %H:%M:%S}")
    begin = time.time()

    data = asyncio.run(main(
        run_funck=partial(execute_run_cabinet,
                          func_name='campaign_query'),
        postprocess_func=group_advert_and_id,
        cabinet={'Галилова': os.getenv('Galilova')}
        # exclude_names=['Мишнева', 'Шелудько',]


    ))

    worksheet = sheets_names()['api_wb_advert']
    save_in_gsh(dict_data=data, worksheet_name=worksheet)

    end = time.time()
    send_tg_message(f"Время выполнения программы:\n{(end-begin)/60:.2f} минут")

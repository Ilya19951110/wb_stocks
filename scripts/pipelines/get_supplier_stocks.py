
from scripts.spreadsheet_tools.push_all_cabinet import push_concat_all_cabinet_stocks_to_sheets
from scripts.postprocessors.group_stocks import merge_and_transform_stocks_with_idkt
from scripts.utils.config.factory import sheets_names, get_requests_url_wb
from scripts.utils.request_block_nmId import get_block_nmId
from scripts.spreadsheet_tools.update_barcode_by_tables import update_barcode
from scripts.spreadsheet_tools.push_mishneva_sheludko import push_stocks_mishneva_sheludko
from scripts.utils.telegram_logger import send_tg_message
from scripts.engine.run_cabinet import execute_run_cabinet
from scripts.utils.setup_logger import make_logger
from scripts.utils.telegram_logger import send_tg_message
from scripts.engine.universal_main import main
from functools import partial
from datetime import datetime
import pandas as pd
import asyncio
import aiohttp
import time
import os


logger = make_logger(__name__, use_telegram=False)


async def get_stocks(session: aiohttp.ClientSession, name: str, api: str) -> pd.DataFrame:
    """
    📦 Скрипт сбора и обработки остатков Wildberries по кабинетам с выгрузкой в Google Таблицы

    Этот модуль автоматически запрашивает данные об остатках (`stocks`) по каждому кабинету WB через API,
    обрабатывает их, добавляет расчетные поля (актуальные цена и скидка), агрегирует, а затем сохраняет:

    1. Итоговые остатки всех кабинетов (с ID KT) — в сводную таблицу
    2. Все баркоды с артикулами поставщика — в отдельный лист
    3. Дополнительно обновляет баркоды в таблицах по маппингу.

    ───────────────────────────────────────────────────────────────────────────────

    🔧 Основные компоненты:

    - `get_stocks()` — асинхронная функция запроса остатков по API Wildberries.
    - `main()` — универсальный исполнитель, который запускает `get_stocks` параллельно для всех кабинетов.
    - `merge_and_transform_stocks_with_idkt()` — объединяет данные с ID карточек.
    - `push_concat_all_cabinet_stocks_to_sheets()` — сохраняет агрегированные остатки в итоговую Google Таблицу.
    - `update_barcode()` — распределяет и обновляет данные по баркодам в зависимости от маппинга кабинетов.

    ───────────────────────────────────────────────────────────────────────────────

    📥 Входные данные:

    - API-ключи для каждого кабинета Wildberries
    - Карта маппинга кабинетов → таблиц (используется в `update_barcode`)
    - Конфиг `sheets_names()` для определения целевых листов

    📤 Выходные таблицы:

    1. `group_stocks_and_idkt` — остатки с ID карточек
    2. `group_all_barcodes` — все артикулы поставщика и баркоды (clear_range = A:D)
    3. Таблицы по группам (например: Фин модель Иосифовы Р А М) — обновление листов с баркодами

    ───────────────────────────────────────────────────────────────────────────────

    📊 Обработка остатков:

    - Приводятся к единому формату
    - Преобразуется дата `lastChangeDate` → `Справка`
    - Извлекаются максимальные цена и скидка по каждому `Артикулу WB`
    - Выводятся актуальные столбцы: "Цена", "Скидка"

    ───────────────────────────────────────────────────────────────────────────────

    🧩 Зависимости:

    - `aiohttp`, `asyncio` — асинхронные запросы
    - `pandas` — обработка DataFrame
    - `dotenv` — конфигурации
    - `gspread`, `gspread_dataframe` — работа с Google Sheets
    - `scripts.utils.config.factory`, `telegram_logger`, `spreadsheet_tools`, `engine.run_cabinet`

    ───────────────────────────────────────────────────────────────────────────────

    🧑‍💻 Автор: Илья  
    📅 Версия: Июль 2025
    """

    url = get_requests_url_wb()['supplier_stocks']

    headers = {"Authorization": api}

    params = {
        "dateFrom": "2024-01-01"
    }
    try:
        async with session.get(url, headers=headers, params=params) as response:
            logger.info(f"🚀🚀  Начинаю запрос ОСТАТКОВ к кабинету {name}")

            if response.status != 200:
                msg = f"⚠️⚠️ Ошибка запроса 'ОСТАТКОВ': {await response.text()}"
                send_tg_message(msg)
                raise ValueError(msg)

            result = await response.json()

    except Exception as e:
        msg = f"❌❌  Произошла ошибка при вызове: {e}"
        logger.error(msg)
        send_tg_message(msg)
        return pd.DataFrame(columns=['nmId', 'barcode'])

    else:
        if not result:
            logger.warning(f"⚠️ Пустой ответ от API для {name}")
            result_error = pd.DataFrame([{
                'Артикул WB': 0,
                'Справка': 'пусто',
                'Бренд': 'пусто',
                'Размер': 'пусто',
                'Итого остатки': 0,
                'Баркод': 0,
                'Цена': 0,
                'Скидка': 0,
                'Артикул поставщика': 0,
            }])
            logger.warning(
                f"⚠️ Для кабинета {name} создан шаблон пустого DataFrame остатков (stocks пустой)")

            return result_error
        data_stoks = pd.DataFrame(result)
    # 1. Создаем основную таблицу с остатками

        data_stoks = data_stoks.rename(columns={
            'nmId': 'Артикул WB',
            'lastChangeDate': 'Дата Обновления',
            'brand': 'Бренд',
            'techSize': 'Размер',
            'quantityFull': 'Итого остатки',
            'barcode': 'Баркод',
            'Price': 'Цена',
            'Discount': 'Скидка',
            'supplierArticle': 'Артикул поставщика'})

        # преобразуем столбец спарвка в нужный формат даты, например 2025-01-01
        data_stoks['Дата Обновления'] = pd.to_datetime(
            data_stoks['Дата Обновления'],  errors='coerce').dt.date

        logger.info(data_stoks['Дата Обновления'])
        # сортируем в порядке убывания
        df_sort = data_stoks.sort_values('Дата Обновления', ascending=False)

        # создаем новый столбец и подставяем туда последнюю актуальную цену

        last_date = data_stoks.groupby('Артикул WB')[
            'Дата Обновления'].transform('max')
        latest_rows = data_stoks[data_stoks['Дата Обновления'] == last_date]

        max_price = (
            latest_rows
            .groupby('Артикул WB', as_index=False)['Цена']
            .max()
            .rename(columns={'Цена': 'Макс_цена'})
        )

        max_discount = (
            latest_rows
            .groupby('Артикул WB', as_index=False)['Скидка']
            .max()
            .rename(columns={'Скидка': 'Макс_скидка'})
        )
        # max_price = (
        #     data_stoks
        #     .sort_values(['Артикул WB', 'Дата Обновления'], ascending=[True, False])
        #     .drop_duplicates('Артикул WB')
        #     [['Артикул WB', 'Цена']]
        #     .rename(columns={'Цена': 'Макс_цена'})
        # )

        # # создаем новый столбец и подставяем туда последнюю актуальную цену скидку
        # max_discount = (
        #     data_stoks
        #     .sort_values(['Артикул WB', 'Дата Обновления'], ascending=[True, False])
        #     .drop_duplicates('Артикул WB')
        #     [['Артикул WB', 'Скидка']]
        #     .rename(columns={'Скидка': 'Макс_скидка'})
        # )

        df_sort = (
            df_sort
            .merge(max_price, on='Артикул WB', how='left')
            .merge(max_discount, on='Артикул WB', how='left')
        )

        df_sort['Цена'] = df_sort['Макс_цена']
        df_sort['Скидка'] = df_sort['Макс_скидка']

        df_sort = df_sort.drop(['Макс_цена', 'Макс_скидка'], axis=1)

        logger.info(
            f"[✅ Остатки {name} успешно обработаны: {len(df_sort)} строк")

        return df_sort


if __name__ == '__main__':

    send_tg_message(
        f"🏁 Скрипт запущен 'get_stocks': {datetime.now():%Y-%m-%d %H:%M:%S}")

    begin = time.time()

    result_data = asyncio.run(main(
        run_funck=partial(execute_run_cabinet,
                          func_name='get_stocks'),
        postprocess_func=merge_and_transform_stocks_with_idkt,
        
        #
    ))

    fileterd_name = ['Мишнева', 'Шелудько']
    mishneva_sheludko_stocks = {
        name: stocks for name, (stocks, _) in result_data.items() if name in fileterd_name}

    stocks_list = [stocks[0] for name,
                   stocks in result_data.items() if name not in fileterd_name]

    article_seller = [barcode[1] for name,
                      barcode in result_data.items() if name not in fileterd_name]

    logger.info(
        f"📦 Подготовлено {len(stocks_list)} датафреймов для выгрузки остатков кабинетов: [{', '.join(f for f in fileterd_name)}]")

    push_stocks_mishneva_sheludko(
        data=mishneva_sheludko_stocks
    )

    if stocks_list or article_seller:
        push_concat_all_cabinet_stocks_to_sheets(
            data=stocks_list,
            sheet_name=sheets_names()['group_stocks_and_idkt'],
            block_nmid=get_block_nmId(),

        )

        logger.info(
            f"📦 Подготовлено {len(article_seller)} датафреймов для выгрузки баркодов")

        push_concat_all_cabinet_stocks_to_sheets(
            data=article_seller,
            sheet_name=sheets_names()['group_all_barcodes'],
            clear_range=['A:D']
        )

        update_barcode(
            data=result_data,

        )
    end = time.time()

    logger.info(f"😎 Время выполнения: {(end-begin)/60:,.2f}")
# py -m scripts.pipelines.get_supplier_stocks

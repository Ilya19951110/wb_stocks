
from scripts.spreadsheet_tools.push_all_cabinet import push_concat_all_cabinet_stocks_to_sheets
from scripts.postprocessors.group_stocks import merge_and_transform_stocks_with_idkt
from scripts.utils.config.factory import sheets_names, get_requests_url_wb
from scripts.utils.request_block_nmId import get_block_nmId
from scripts.spreadsheet_tools.update_barcode_by_tables import update_barcode
from scripts.utils.telegram_logger import send_tg_message
from scripts.engine.run_cabinet import execute_run_cabinet
from scripts.utils.setup_logger import make_logger
from scripts.engine.universal_main import main
from functools import partial
from datetime import datetime
import pandas as pd
import asyncio
import aiohttp
import time


logger = make_logger(__name__, use_telegram=False)


async def get_stocks(session: aiohttp.ClientSession, name: str, api: str) -> pd.DataFrame:
    url = get_requests_url_wb()['supplier_stocks']

    headers = {"Authorization": api}
    # тело запроса
    params = {
        "dateFrom": "2024-01-01"  # datetime.now().strftime('%Y-%m-%d')
    }
    try:
        async with session.get(url, headers=headers, params=params) as response:
            logger.info(f"🚀🚀  Начинаю запрос ОСТАТКОВ к кабинету {name}")

            if response.status != 200:
                raise ValueError(f"⚠️⚠️ Ошибка запроса 'ОСТАТКОВ': {await response.text()}")

            result = await response.json()

    except Exception as e:
        logger.error(f"❌❌  Произошла ошибка при вызове: {e}")
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
            'lastChangeDate': 'Справка',
            'brand': 'Бренд',
            'techSize': 'Размер',
            'quantityFull': 'Итого остатки',
            'barcode': 'Баркод',
            'Price': 'Цена',
            'Discount': 'Скидка',
            'supplierArticle': 'Артикул поставщика'})
        # преобразуем столбец спарвка в нужный формат даты, например 2025-01-01
        data_stoks['Справка'] = pd.to_datetime(
            data_stoks['Справка'], format='ISO8601').dt.date

        # сортируем в порядке убывания
        df_sort = data_stoks.sort_values('Справка', ascending=False)

        # создаем новый столбец и подставяем туда последнюю актуальную цену
        max_price = df_sort.loc[df_sort.groupby('Артикул WB',)[
            'Цена'].idxmax()]

        # создаем новый столбец и подставяем туда последнюю актуальную цену скидку
        max_discount = df_sort.loc[df_sort.groupby('Артикул WB')[
            'Скидка'].idxmax()]

        df_sort = df_sort.merge(
            max_price[[
                'Артикул WB', 'Цена'
            ]].rename(columns={'Цена': 'Макс_цена'}),
            on='Артикул WB',
            how='left'

        ).merge(
            max_discount[[
                'Артикул WB', 'Скидка'
            ]].rename(columns={'Скидка': 'Макс_скидка'}),
            on='Артикул WB',
            how='left'
        )

        df_sort['Цена'] = df_sort['Макс_цена']
        df_sort['Скидка'] = df_sort['Макс_скидка']

        df_sort = df_sort.drop(['Макс_цена', 'Макс_скидка'], axis=1)

        logger.info(
            f"[✅ Остатки {name} успешно обработаны: {len(df_sort)} строк")

        return df_sort


if __name__ == '__main__':
    # запуск
    # python -m scripts.pipelines.get_supplier_stocks

    send_tg_message(
        f"🏁 Скрипт запущен 'get_stocks': {datetime.now():%Y-%m-%d %H:%M:%S}")

    begin = time.time()

    result_data = asyncio.run(main(
        run_funck=partial(execute_run_cabinet,
                          func_name='get_stocks'),
        postprocess_func=merge_and_transform_stocks_with_idkt,
        cache_name="test_cache.pkl"
    ))

    stocks_list = [stocks[0] for stocks in result_data.values()]

    article_seller = [barcode[1] for barcode in result_data.values()]

    logger.info(
        f"📦 Подготовлено {len(stocks_list)} датафреймов для выгрузки остатков")

    push_concat_all_cabinet_stocks_to_sheets(
        data=stocks_list,
        sheet_name=sheets_names()['group_stocks_and_idkt'],
        block_nmid=get_block_nmId()
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

    print(f"😎 Время выполнения: {(end-begin)/60:,.2f}")

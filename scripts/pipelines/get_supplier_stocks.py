
from scripts.spreadsheet_tools.push_all_cabinet import push_concat_all_cabinet_stocks_to_sheets
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
import time
import pickle


logger = make_logger(__name__, use_telegram=False)


async def get_stocks(session, name, api):
    url = "https://statistics-api.wildberries.ru/api/v1/supplier/stocks"

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
            f"[92m✅ Остатки {name} успешно обработаны: {len(df_sort)} строк")

        return df_sort


def merge_and_transform_stocks_with_idkt(stocks, IDKT, name):

    try:
        logger.warning('📂 Читаем папку cache')

        with open(f"cache/{name}_cards", 'rb') as f:
            df_idkt = pickle.load(f)

        important_cols = ['Артикул WB', 'Бренд']
        short_df = stocks[important_cols].head(5)

        logger.info(
            f"📊 DF (сокращённый stocks {name}):\n{short_df.to_string(index=False)}")

    except Exception as e:
        logger.error(f"❌ Ошибка чтения chace:\n{e}")

    try:
        logger.info(
            "📊 Приводим типы данных в столбцах [Артикул WB, Баркод, Размер, ID KT]...")

        type_map = {
            'Артикул WB': int,
            'Баркод': str,
            'Размер': str
        }
        for df in [stocks, IDKT]:
            for col, dtype in type_map.items():
                df[col] = df[col].astype(dtype)

        IDKT['ID KT'] = IDKT['ID KT'].astype(int)

        logger.info("✅ Типы данных успешно приведены!")
    except Exception as e:
        logger.error(
            f"❌ Не удалось привести типы данных {name}: {e}")
        # Объединяем две таблицы остатки цепляем к idkt
    try:
        logger.info("🔗 Выполняем объединение таблиц (merge)...")

        result = pd.merge(
            IDKT,
            stocks,
            on=['Артикул WB', 'Баркод'],
            how='outer',
            indicator=True,
            suffixes=('_IDKT', '_stocks')
        )
        logger.info("✅ Объединение выполнено успешно!")

    except Exception as e:
        logger.error(f"❌ Не удалось объединить таблицы: {e}")

    try:
        logger.info("🧹 Начинаем финальную очистку и обработку данных...")
        # Удаляем не нужные столбцы
        result = result.drop(columns=[col for col in result.columns if col.endswith('_stocks')]+['Справка', 'warehouseName',
                                                                                                 'quantity', 'inWayToClient', 'inWayFromClient',
                                                                                                 'category', 'subject', 'isRealization', 'SCCode', 'isSupply'], errors='ignore')

        # удаляем суффиксы _IDKT у столбцов, которые остались
        result.columns = [
            col.replace('_IDKT', '') for col in result.columns
        ]

        # выбираем столбцы
        num_col = ['Цена', 'Скидка',
                   'Итого остатки', 'Ширина', 'Высота', 'Длина']
        string_cols = ['Бренд', 'Размер', 'Категория', 'Наименование',
                       ]

        # Заполняем NAN в Цена и Скидка последними известными знач для артикула
        result[['Цена', 'Скидка']] = result.groupby(
            'Артикул WB')[['Цена', 'Скидка']].ffill()
        # заполняем пустоты нужными значениями
        result[num_col] = result[num_col].fillna(0)
        result[string_cols] = result[string_cols].fillna('-')

        # сохраняем только те строки, которые есть в таблице stocks остатки
        right_only_rows = result[result['_merge'] == 'right_only']

        # в осноном дф удаляем строки которые есть только в правой таблице, они косячные
        result = result[result['_merge'] != 'right_only']

        # удаляем столбец _merge
        result = result.drop(columns='_merge')

        # группируем по столбцу итого остатки
        result = result.groupby([
            col for col in result.columns if col != 'Итого остатки'
        ])['Итого остатки'].sum().reset_index()

        # Создаем новый столбец Цена до СПП
        result['Цена до СПП'] = result['Цена'] * \
            (1 - result['Скидка']/100)

        # дф с артикулом и баркодом
        barcode_nmid = result.filter([
            'Артикул WB', 'Баркод', 'Размер'
        ])

        seller_article = result.filter([
            'Артикул WB', 'Баркод', 'Артикул поставщика', 'Размер'
        ])

        result = result.drop(columns=[
            'Баркод', 'Размер'
        ])

        result[['Цена', 'Скидка', 'Цена до СПП']] = result.groupby(
            'Артикул WB')[['Цена', 'Скидка', 'Цена до СПП']].transform('first')

        # группировка после удаления по сумме остатков
        result = result.groupby([
            col for col in result.columns if col != 'Итого остатки'
        ])['Итого остатки'].sum().reset_index()

        new_order = [
            'Артикул WB', 'ID KT', 'Артикул поставщика', 'Бренд', 'Наименование', 'Категория',
            'Итого остатки', 'Цена', 'Скидка', 'Цена до СПП', 'Фото', 'Ширина', 'Высота', 'Длина'
        ]

        # применяем новое расположение
        result = result[new_order]

        result = result.sort_values('Итого остатки', ascending=False)
        result['Кабинет'] = name

        if len(right_only_rows) > 0:
            logger.warning(
                f"косячная карточка кабинета {name} =  {right_only_rows.shape}\n{right_only_rows['Артикул WB'].to_list()}")

        else:
            logger.info(f"✅ Косячных карточек в {name} не найдено")

        logger.warning(
            f"🟢 Есть ли дубликаты в {name}?: {result.duplicated().any()}\n"
            f"📦 Колонки barcode_nmid: {barcode_nmid.columns.tolist()}"
        )

    except Exception as e:
        logger.error(
            f"❌ Ошибка в процессе обработки данных: {e}")

    return result, seller_article, barcode_nmid


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
        sheet_name='API',
        block_nmid=get_block_nmId()
    )

    logger.info(
        f"📦 Подготовлено {len(article_seller)} датафреймов для выгрузки баркодов")
    push_concat_all_cabinet_stocks_to_sheets(
        data=article_seller,
        sheet_name='API 2',
        clear_range=['A:D']
    )

    update_barcode(
        data=result_data,

    )
    end = time.time()

    print(f"😎 Время выполнения: {(end-begin)/60:,.2f}")

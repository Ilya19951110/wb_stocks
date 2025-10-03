from scripts.utils.telegram_logger import send_tg_message
from scripts.utils.setup_logger import make_logger
import pandas as pd

logger = make_logger(__name__, use_telegram=False)


def merge_and_transform_stocks_with_idkt(stocks: pd.DataFrame, IDKT: pd.DataFrame, name: str) -> tuple[pd.DataFrame, pd.DataFrame]:
  

    stocks['Дата Обновления'] = pd.to_datetime(
            stocks['Дата Обновления'],  errors='coerce').dt.date

    logger.info(stocks['Дата Обновления'])
        # сортируем в порядке убывания
    df_sort = stocks.sort_values('Дата Обновления', ascending=False)

        # создаем новый столбец и подставяем туда последнюю актуальную цену
    last_date = stocks.groupby('Артикул WB')[
            'Дата Обновления'].transform('max')
        
    latest_rows = stocks[stocks['Дата Обновления'] == last_date]

       
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
    df_sort = (
            df_sort
            .merge(max_price, on='Артикул WB', how='left')
            .merge(max_discount, on='Артикул WB', how='left')
        )

    df_sort['Цена'] = df_sort['Макс_цена']
    df_sort['Скидка'] = df_sort['Макс_скидка']
    df_sort = df_sort.drop(['Макс_цена', 'Макс_скидка',], axis=1)

    if name not in ('Мишнева', 'Шелудько'):
        logger.warning(f"{name} зашли в дату обновления last_date")
        df_sort['Дата Обновления'] = last_date

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
        msg = f"❌ Не удалось привести типы данных {name}: {e}"
        send_tg_message(msg)
        logger.error(msg)
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
        msg = f"❌ Не удалось объединить таблицы: {e}"
        send_tg_message(msg)
        logger.error(msg)

    try:
        logger.info("🧹 Начинаем финальную очистку и обработку данных...")
        # Удаляем не нужные столбцы
        result = result.drop(columns=[col for col in result.columns if col.endswith('_stocks')]+['warehouseName',
                                                                                                 'inWayToClient', 'inWayFromClient',
                                                                                                 'category', 'subject', 'isRealization', 'SCCode', 'isSupply'], errors='ignore')

        # удаляем суффиксы _IDKT у столбцов, которые остались
        result.columns = [
            col.replace('_IDKT', '') for col in result.columns
        ]

        # выбираем столбцы
        num_col = ['Цена', 'Скидка',
                   'Итого остатки', 'Ширина', 'Высота', 'Длина', 'quantity']
        string_cols = ['Бренд', 'Размер', 'Категория', 'Наименование',
                       ]

        # Заполняем NAN в Цена и Скидка последними известными знач для артикула
        result['Дата Обновления'] = result['Дата Обновления'].astype(str)

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
        logger.debug('Группируем Итого остатки')
        result = result.groupby([
            col for col in result.columns if col != 'Итого остатки' and col !='quantity'
        ]).agg({
            'quantity': 'sum',
            'Итого остатки':'sum'
        }).reset_index()

        logger.debug(result.columns.tolist())
        logger.info(result.head(5))
        # ['Итого остатки'].sum().reset_index()
        # Создаем новый столбец Цена до СПП
        result['Цена до СПП'] = result['Цена'] * \
            (1 - result['Скидка']/100)

        # дф с артикулом и баркодом

        seller_article = result.filter([
            'Артикул WB', 'Баркод', 'Артикул поставщика', 'Размер'
        ])

        # result = result.drop(columns=[
        #     'Баркод', 'Размер'
        # ])

        result[['Цена', 'Скидка', 'Цена до СПП']] = result.groupby(
            'Артикул WB')[['Цена', 'Скидка', 'Цена до СПП']].transform('first')

        # группировка после удаления по сумме остатков
        result = result.groupby([
            col for col in result.columns if col != 'Итого остатки'
        ])['Итого остатки'].sum().reset_index()

        result['Кабинет'] = name
        
        new_order = [
            'Артикул WB', 'ID KT', 'Артикул поставщика', 'Бренд', 'Наименование', 'Категория',
            'Итого остатки', 'Цена', 'Скидка', 'Цена до СПП', 'Фото', 'Ширина', 'Высота', 'Длина', 'Кабинет', 'Баркод', 'Размер', 'Дата Обновления', 'quantity'
        ]

        # применяем новое расположение
        result = result[new_order].rename(columns={'quantity': 'Остатки'})
      

        result = result.sort_values('Итого остатки', ascending=False)
        

        if len(right_only_rows) > 0:
            logger.warning(
                f"косячная карточка кабинета {name} =  {right_only_rows.shape}\n{right_only_rows['Артикул WB'].to_list()}")

        else:
            logger.info(f"✅ Косячных карточек в {name} не найдено")

        logger.warning(
            f"🟢 Есть ли дубликаты в {name}?: {result.duplicated().any()}\n"
            f"📦 Колонки barcode_nmid: {seller_article.columns.tolist()}"
        )

    except Exception as e:
        msg = f"❌ Ошибка в процессе обработки данных: {e}"
        send_tg_message(msg)
        logger.error(msg)

    logger.debug(f"\n{name} -> {result.columns.tolist()}")
    if name in ('Мишнева', 'Шелудько'):
        result = result.drop(columns=['Дата Обновления', 'Остатки',  'Баркод', 'Размер'])

    return result, seller_article

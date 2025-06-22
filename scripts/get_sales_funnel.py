
from dotenv import load_dotenv
from scripts.setup_logger import make_logger
from scripts.telegram_logger import send_tg_messege
import pandas as pd
import json
import time
from datetime import datetime, timedelta
import asyncio
from functools import partial
from scripts import run_cabinet

load_dotenv()

logger = make_logger(__name__)


async def report_detail(name, api, session):

    stop, page, all_data = 21, 1, []
    url = 'https://seller-analytics-api.wildberries.ru/api/v2/nm-report/detail'
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
            async with session.post(url, headers=headers, json=params) as result:
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


def get_current_week_sales_df(sales, ID, name):

    def read_to_json(data, parent_key='', sep='_'):

        items = []

        for k, v in data.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(read_to_json(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))

        return dict(items)

    data = [read_to_json(item) for item in sales]
    df = pd.DataFrame(data)

    assert isinstance(df, pd.DataFrame), "Входной df должен быть DataFrame"

    columns_rus = [
        'Артикул WB', 'Артикул поставщика', 'Бренд', 'ID категории', 'Название категории', 'Начало текущего периода', 'Конец текущего периода', 'Просмотры карточки', 'Добавления в корзину',
        'Количество заказов', 'Сумма заказов (руб)', 'Количество выкупов', 'Сумма выкупов (руб)', 'Количество отмен', 'Сумма отмен (руб)', 'Среднее заказов в день', 'Средняя цена (руб)',
        'Конверсия в корзину (%)', 'Конверсия в заказ (%)', 'Конверсия в выкуп (%)', 'Начало предыдущего периода', 'Конец предыдущего периода', 'Просмотры карточки (пред.)', 'Добавления в корзину (пред.)',
        'Количество заказов (пред.)', 'Сумма заказов (пред., руб)', 'Количество выкупов (пред.)', 'Сумма выкупов (пред., руб)', 'Количество отмен (пред.)', 'Сумма отмен (пред., руб)', 'Среднее заказов в день (пред.)',
        'Средняя цена (пред., руб)', 'Конверсия в корзину (пред., %)', 'Конверсия в заказ (пред., %)', 'Конверсия в выкуп (пред., %)', 'Динамика просмотров (%)', 'Динамика корзины (%)',
        'Динамика заказов (%)', 'Динамика суммы заказов (%)', 'Динамика выкупов (%)', 'Динамика суммы выкупов (%)', 'Динамика отмен (%)', 'Динамика суммы отмен (%)', 'Динамика ср. заказов в день (%)',
        'Динамика ср. цены (%)', 'Изменение конверсии в корзину', 'Изменение конверсии в заказ', 'Изменение конверсии в выкуп', 'Остатки на маркетплейсе', 'Остатки на WB'
    ]

    df = df.rename(
        columns=dict(zip(df.columns.tolist(), columns_rus))
    )

    date_col = ['Начало текущего периода', 'Конец текущего периода',
                'Начало предыдущего периода', 'Конец предыдущего периода']

    df[date_col] = df[date_col].apply(
        pd.to_datetime, errors='coerce').apply(lambda x: x.dt.date)

    current_week_col = df.columns[:df.columns.get_loc(
        'Начало предыдущего периода')].tolist() + df.columns[-3:].tolist()

    current_week = df[current_week_col].copy()

    current_week['Начало текущего периода'] = pd.to_datetime(
        current_week['Начало текущего периода'])

    current_week['Номнед'] = current_week['Начало текущего периода'].dt.isocalendar().week

    current_week['Начало текущего периода'] = pd.to_datetime(
        current_week['Начало текущего периода']).dt.date

    final_df = pd.merge(
        current_week,
        ID,
        left_on='Артикул WB',
        right_on='Артикул WB',
        how='left',
        indicator=True
    )

    final_df['Кабинет'] = name
    final_df['ID KT'] = final_df['ID KT'].fillna(0)

    final_df = final_df.drop_duplicates()
    # ID KT
    final_df = final_df.rename(columns={
        'ID KT': 'ID',
        'Номнед': 'Неделя',
        'Просмотры карточки': 'Переходы в карточку',
        'Добавления в корзину': 'Положили в корзину',
        'Количество заказов': 'Заказали, шт',
        'Количество выкупов': 'Выкупили, шт',
        'Количество отмен': 'Отменили, шт',
        'Сумма заказов (руб)': 'Заказали на сумму, руб',

    }).filter([
        'ID', 'Неделя', 'Переходы в карточку',	'Положили в корзину',	'Заказали, шт',	'Выкупили, шт', 'Отменили, шт',	'Заказали на сумму, руб',
    ]).groupby([
        'ID', 'Неделя',
    ]).agg({
        'Переходы в карточку': 'sum',
        'Положили в корзину': 'sum',
        'Заказали, шт': 'sum',
        'Выкупили, шт': 'sum',
        'Отменили, шт': 'sum',
        'Заказали на сумму, руб': 'sum',
    }).reset_index()

    logger.info(final_df.head(5))

    return final_df


if __name__ == '__main__':

    from scripts.upload_to_google_sheet import save_in_gsh
    from scripts.universal_main import main
    send_tg_messege(
        f"🏁 Скрипт запущен 'report_detail': {datetime.now():%Y-%m-%d %H:%M:%S}")
    begin = time.time()

    data = asyncio.run(main(
        run_funck=partial(run_cabinet.execute_run_cabinet,
                          func_name='report_detail'),
        postprocess_func=get_current_week_sales_df,
        cache_name="test_cache.pkl"
    ))

    save_in_gsh(dict_data=data, wks_name='API WB Воронка')

    end = time.time()
    print(f"⏱ Выполнено за {(end - begin)/60:.2f} минут")

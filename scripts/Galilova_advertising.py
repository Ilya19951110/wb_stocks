import gspread
from datetime import datetime, timedelta
import pandas as pd
import json
import pandas as pd
import requests
import os
import time
from scripts.gspread_client import get_gspread_client
#  Таблица менеджера. Галилова


def funnel_sales_Galilova():
    stop, page, url = 21, 1, 'https://seller-analytics-api.wildberries.ru/api/v2/nm-report/detail'
    all_data = []

    while True:

        headers = {
            'Authorization': os.getenv('Galilova').strip(),
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
        res = requests.post(url, headers=headers, json=params)
        print(f"подключение к Galilova: {res}")

        result = res.json()

        cards = result['data']['cards']
        all_data.extend(cards)
        page += 1

        print(
            f'Получено {len(cards)} записей кабинета Galilova. Всего: {len(all_data)}')
        if len(cards) < 1000:
            break

        print(f'Спим {stop} сек')
        time.sleep(stop)

    return all_data


def read_to_json(data, parent_key='', sep='_'):
    print(data)
    items = []

    for k, v in data.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(read_to_json(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))

    return dict(items)


def galilova_sales_weekly_agg():

    data = [read_to_json(item) for item in funnel_sales_Galilova()]
    df = pd.DataFrame(data)

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

    for col in date_col:
        df[col] = pd.to_datetime(
            df[col], format='%Y-%m-%d  %H:%M:%S', errors='coerce').dt.date

    current_week_col = df.columns[:df.columns.get_loc(
        'Начало предыдущего периода')].tolist() + df.columns[-3:].tolist()

    current_week = df[current_week_col].copy()

    current_week['Начало текущего периода'] = pd.to_datetime(
        current_week['Начало текущего периода'])

    current_week['Номнед'] = current_week['Начало текущего периода'].dt.isocalendar().week

    current_week['Начало текущего периода'] = pd.to_datetime(
        current_week['Начало текущего периода']).dt.date

    print(f"до группировки {len(current_week)}")
    current_week = current_week.drop_duplicates()

    current_week = current_week.rename(columns={
        'Номнед': 'Неделя',
        'Просмотры карточки': 'Переходы в карточку',
        'Добавления в корзину': 'Положили в корзину',
        'Количество заказов': 'Заказали, шт',
        'Количество выкупов': 'Выкупили, шт',
        'Количество отмен': 'Отменили, шт',
        'Сумма заказов (руб)': 'Заказали на сумму, ₽',

    }).filter([
        'Артикул WB', 'Неделя', 'Переходы в карточку',	'Положили в корзину',	'Заказали, шт',	'Выкупили, шт', 'Отменили, шт',	'Заказали на сумму, ₽',
    ]).groupby([
        'Артикул WB', 'Неделя',
    ]).agg({
        'Переходы в карточку': 'sum',
        'Положили в корзину': 'sum',
        'Заказали, шт': 'sum',
        'Выкупили, шт': 'sum',
        'Отменили, шт': 'sum',
        'Заказали на сумму, ₽': 'sum',
    }).reset_index()

    return current_week


def manager_table(data):

    gs = get_gspread_client()

    sh = gs.open('Таблица менеджера. Галилова')
    sheet = sh.worksheet('Артикулы, для запроса API')
    sheet_sales = sh.worksheet('API WB Воронка')

    filtered_nmID = data[data['Артикул WB'].isin(
        pd.Series(sheet.col_values(1)).astype(int))].reset_index(drop=True)

    sheet_sales.update(
        range_name=f"A{len(sheet_sales.get_all_values())+1}",
        values=filtered_nmID.values.tolist()
    )
    print(f"Данные в таблицу {sheet_sales} выгружены!")


if __name__ == '__main__':

    combain = galilova_sales_weekly_agg()
    manager_table(combain)

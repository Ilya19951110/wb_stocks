from collections import defaultdict
import gspread
import pandas as pd
import json
import time
import os
from datetime import datetime, timedelta
import requests
import openpyxl
from openpyxl.utils.dataframe import dataframe_to_rows


def query_in_idkt(cabinet, hdrs):
    DELAY = 0.7  # задержка при выполнении кода
    #
    url_cards = "https://content-api.wildberries.ru/content/v2/get/cards/list"

    # итоговый список карточек
    all_cards = []
    rows = []
    cursor = None

    while True:
        headers = {"Authorization": hdrs}
        # Формируем тело запроса
        payload = {
            "settings": {
                "sort": {"ascending": False},
                "filter": {"withPhoto": -1},
                "cursor": {"limit": 100},
                "period": {
                    'begin': '2024-01-01',
                    'end': datetime.now().strftime("%Y-%m-%d")
                },
            },
        }

        # Добавляем cursor для пагинации
        if cursor:
            payload["settings"]["cursor"].update(cursor)

        # Отправляем запрос
        response = requests.post(url_cards, headers=headers, json=payload)
        print(
            f'подключение к content/v2/get/cards/list:\n {response}, {cabinet}')
        # проверяем статус запроса 200 -  это успешное подключение
        if response.status_code != 200:
            print(f"Ошибка: {response.status_code}")
            print(response.text)
            break

        data = response.json()

        # Проверяем структуру ответа
        if 'cards' not in data or 'cursor' not in data:
            print("Некорректный формат ответа:")
            print(json.dumps(data, indent=4, ensure_ascii=False))
            break

        # перебераем данные в словре data'cards'
        for card in data['cards']:
            # проверяет, является ли текущий элемент card словарем
            if isinstance(card, dict):
                # создаем новый словарь
                all_cards.append({
                    # берем все пары key:val из card
                    key: val for key, val in card.items()
                    # исключаем key description - это описание
                    if key not in ['description']
                })

        if not data or 'cards' not in data or len(data['cards']) < 100:
            # if len(data['cards']) < 100:
            break

        # Обновляем курсор
        cursor = {
            "updatedAt": data['cursor']['updatedAt'],
            "nmID": data['cursor']['nmID']
        }

        time.sleep(DELAY)

    if all_cards:
        for card in all_cards:
            info = {
                'Артикул WB': card['nmID'],
                'ID': card['imtID'],
            }
            rows.append(info)

    return pd.DataFrame(rows)


def funnel_sales(name, hdrs):
    stop = 21
    page = 1
    url = 'https://seller-analytics-api.wildberries.ru/api/v2/nm-report/detail'
    all_data = []
    while True:

        now = datetime.now()
        headers = {
            'Authorization': hdrs,
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        params = {

            'timezone': 'Europe/Moscow',
            'period': {
                # Полный формат
                # (now - timedelta(days=10)).strftime('%Y-%m-%d 00:00:00')
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
        print(f"{name} подключение {res}")

        result = res.json()

        cards = result['data']['cards']

        all_data.extend(cards)
        page += 1
        print(
            f'Получено {len(cards)} записей кабинета {name}. Всего: {len(all_data)}')
        if len(cards) < 1000:
            break

        print(f'Спим {stop} сек')
        time.sleep(stop)

    return all_data


def read_to_json(data, parent_key='', sep='_'):
    items = []

    for k, v in data.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(read_to_json(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))

    return dict(items)


def funck(name, hdrs, IDKT):

    data = [read_to_json(item) for item in funnel_sales(name, hdrs)]
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

    final_df = pd.merge(
        current_week,
        IDKT,
        left_on='Артикул WB',
        right_on='Артикул WB',
        how='left',
        indicator=True
    )

    final_df['Кабинет'] = name
    final_df['ID'] = final_df['ID'].fillna(0)

    print(f"до группировки {len(final_df)}")
    final_df = final_df.drop_duplicates()

    final_df = final_df.rename(columns={
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

    print(final_df.head(5))

    return final_df


def save_in_gsh(dict_data):

    gs = gspread.service_account(
        filename='key.json')

    sheet = 'API WB Воронка'

    data = defaultdict(pd.DataFrame)
    for name, df in dict_data.items():
        if name in ['Азарья', 'Михаил', 'Рахель']:
            data['Фин модель Иосифовы Р А М'] = pd.concat(
                [data['Фин модель Иосифовы Р А М'], df], ignore_index=True)

        if name in ['Галилова']:
            data['Фин модель Галилова'] = pd.concat(
                [data['Фин модель Галилова'], df], ignore_index=True)

        if name in ['Мартыненко', 'Торгмаксимум']:
            data['Фин модель Мартыненко и Торгмаксимум'] = pd.concat(
                [data['Фин модель Мартыненко и Торгмаксимум'], df], ignore_index=True)

    for name, df in data.items():
        sh = gs.open(name)
        worksheet = sh.worksheet(sheet)

        worksheet.update(
            range_name=f"A{len(worksheet.get_all_values())+1}",
            values=df.values.tolist())

    print(f"выгружено в гугл таблицу!")


if __name__ == '__main__':

    result = {}
    all_iosifovy = {
        'Азарья': os.getenv('Azarya'),
        'Михаил': os.getenv('Michael'),
        'Рахель': os.getenv('Rachel'),
        'Галилова': os.getenv('Galilova'),
        'Мартыненко': os.getenv('Martynenko'),
        'Сергей': os.getenv('Sergey'),
        'Торгмаксимум': os.getenv('TORGMAKSIMUM')
    }

    start_time = time.time()

    for name, api in all_iosifovy.items():
        data = funck(
            name=name,
            hdrs=api,
            IDKT=query_in_idkt(
                cabinet=name,
                hdrs=api

            )
        )

        result[name] = data

    for name, df in result.items():
        print(f"Проверка {name}: {len(df)} строк")

    save_in_gsh(result)
    end_time = time.time()
    print(f"Время выполнения: {(end_time-start_time)/60}")

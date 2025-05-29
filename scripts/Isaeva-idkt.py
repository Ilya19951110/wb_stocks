from collections import defaultdict
import requests
import json
from datetime import datetime, timedelta
import pandas as pd
from dotenv import load_dotenv
import os
import time
import math
import gspread
from google.oauth2.service_account import Credentials

# Метод предоставляет количество остатков товаров на складах WB. Данные обновляются раз в 30 минут. Нужен!!!


def query_stocks():

    url = "https://statistics-api.wildberries.ru/api/v1/supplier/stocks"

    headers = {"Authorization": os.getenv('Isaeva')}
    # тело запроса
    params = {
        "dateFrom": "2024-01-01"  # datetime.now().strftime('%Y-%m-%d')
    }
    # запрос
    res = requests.get(url, headers=headers, params=params)
    print(f'подключение к api/v1/supplier/stocks:\n{res}')
    result = res.json()
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
    lasted_max_price = df_sort.loc[df_sort.groupby('Артикул WB',)[
        'Цена'].idxmax()]

    # создаем новый столбец и подставяем туда последнюю актуальную цену скидку
    lasted_min_discount = df_sort.loc[df_sort.groupby('Артикул WB')[
        'Скидка'].idxmax()]

    df_sort = df_sort.merge(
        lasted_max_price[[
            'Артикул WB', 'Цена'
        ]].rename(columns={'Цена': 'Макс_цена'}),
        on='Артикул WB',
        how='left'

    )

    df_sort = df_sort.merge(
        lasted_min_discount[[
            'Артикул WB', 'Скидка'
        ]].rename(columns={'Скидка': 'Макс_скидка'}),
        on='Артикул WB',
        how='left'
    )

    df_sort['Цена'] = df_sort['Макс_цена']
    df_sort['Скидка'] = df_sort['Макс_скидка']

    df_sort = df_sort.drop(['Макс_цена', 'Макс_скидка'], axis=1)

    print(f'Остатки Isaeva сохранены')

    return df_sort


# ----------------------------------------------------------------------------------------------------------------


# Метод предоставляет список созданных карточек товаров.
# с последующией распаковкой в дата фрейм
def query_in_idkt():
    DELAY = 0.7  # задержка при выполнении кода
    #
    url_cards = "https://content-api.wildberries.ru/content/v2/get/cards/list"

    # итоговый список карточек
    all_cards = []
    rows = []
    cursor = None

    while True:
        headers = {"Authorization": os.getenv('Isaeva')}
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
            f'подключение к content/v2/get/cards/list:\n {response}, Isaeva')
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

    print(f"длина all_cards {len(all_cards)}")
    print(f'Список карточек Isaeva выгружен')

    if all_cards:
        for card in all_cards:
            info = {
                'Артикул WB': card['nmID'],
                'ID КТ': card['imtID'],
                'Наименование': card['title'],
                'Артикул поставщика': card['vendorCode'],
                'Бренд': card['brand'],
                'Категория': card['subjectName'],
                'Фото': card['photos'][0]['big'] if card.get('photos') else None,
                'Ширина': card['dimensions']['width'],
                'Высота': card['dimensions']['height'],
                'Длина': card['dimensions']['length']
            }
            # Распаковываем соварь sizes, нам нужен размер и баркод
            for size in card.get('sizes', []):
                for barcode in size.get('skus', []) or [None]:
                    row = info.copy()
                    row.update({
                        'Размер': size.get('techSize'),
                        'chrtID': size.get('chrtID'),
                        'Баркод': barcode if barcode else None
                    })
                rows.append(row)
    # преобразуем в датафрейм
    result = pd.DataFrame(rows)
    #  Оставляем только нужные столбцы в датафрейм
    res_idkt_save = result.filter(
        ['Артикул WB', 'ID КТ', 'Наименование', 'Бренд', 'Размер', 'Баркод', 'Артикул поставщика', 'Категория', 'Фото', 'Ширина', 'Высота', 'Длина'])

    print(f'карточки товаров Isaeva распакованы')
    return res_idkt_save


# ---------------------


# объединение остатков  с id кт
def combain_query(stocks, IDKT):

    stocks['Артикул WB'] = stocks['Артикул WB'].astype(int)
    stocks['Баркод'] = stocks['Баркод'].astype(str)
    stocks['Размер'] = stocks['Размер'].astype(str)

    IDKT['Артикул WB'] = IDKT['Артикул WB'].astype(int)
    IDKT['Баркод'] = IDKT['Баркод'].astype(str)
    IDKT['Размер'] = IDKT['Размер'].astype(str)
    IDKT['ID КТ'] = IDKT['ID КТ'].astype(int)

    # Объединяем две таблицы остатки цепляем к idkt
    result = pd.merge(
        IDKT,
        stocks,
        on=['Артикул WB', 'Баркод'],
        how='outer',
        indicator=True,
        suffixes=('_IDKT', '_stocks')
    )

    # Удаляем не нужные столбцы
    result = result.drop(columns=[col for col in result.columns if col.endswith('_stocks')]+['Справка', 'warehouseName',
                         'quantity', 'inWayToClient', 'inWayFromClient', 'category', 'subject', 'isRealization', 'SCCode', 'isSupply'])

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
        'Артикул WB', 'Баркод'
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
        'Артикул WB', 'ID КТ', 'Артикул поставщика', 'Бренд', 'Наименование', 'Категория',
        'Итого остатки', 'Цена', 'Скидка', 'Цена до СПП', 'Фото', 'Ширина', 'Высота', 'Длина'
    ]

    # применяем новое расположение
    result = result[new_order]
    # фильтруем по убыванию
    result = result.sort_values('Итого остатки', ascending=False)

    if len(right_only_rows) > 0:
        print(f"косячная карточка кабинета Isaeva =  {right_only_rows.shape}",
              right_only_rows['Артикул WB'], sep='\n')

    else:
        print(f'косячных карточек Isaeva нет', '', sep='\n')

    print(f"Есть ли дубликаты в Isaeva?", result.duplicated().any())

    return result, barcode_nmid


def save_in_gsh(dick_data):
    gs = gspread.service_account(
        filename=r'C:\Users\Ilya\OneDrive\Рабочий стол\my_project\myanaliticmp-0617169ebf44.json')

    for sh, (df, _) in dick_data.items():
        spreadsheet = gs.open(sh)
        sheets = spreadsheet.worksheet('API')
        sheets.clear()
        df = df[df['Итого остатки'] > 0]
        sheets.update([df.columns.values.tolist()] + df.values.tolist())

    print('данные загружены в Исаеву')


if __name__ == '__main__':
    Isaeva = {}

    Isaeva['Ассортиментная матрица. Исаева'] = combain_query(
        stocks=query_stocks(),
        IDKT=query_in_idkt()
    )
    print(Isaeva.keys())
    save_in_gsh(Isaeva)

if __name__ == '__main__':
    Isaeva = {}

    Isaeva['Ассортиментная матрица. Исаева'] = combain_query(
        stocks=query_stocks(),
        IDKT=query_in_idkt()
    )

    save_in_gsh(Isaeva)

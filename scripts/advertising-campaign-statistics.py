from collections import defaultdict
import numpy as np
import time
import requests
import json
from datetime import datetime, timedelta
import os
import pandas as pd
import gspread


# Выгрузка из аурум СТАТИСТИКА РК
def query_idkt(hdrs, cabinet):
    url_cards = "https://content-api.wildberries.ru/content/v2/get/cards/list"
    DELAY = 0.7
    all_cards, cursor = [], None

    while True:
        headers = {"Authorization": hdrs}

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

        if cursor:
            payload['settings']['cursor'].update(cursor)

        response = requests.post(url_cards, headers=headers, json=[payload])
        print(
            f'подключение к content/v2/get/cards/list:\n {response}, {cabinet}')

        if response.status_code != 200:
            print(f"Ошибка: {response.status_code}")
            print(response.text)
            break

        data = response.json()

        if 'cards' not in data or 'cursor' not in data:
            print("Некорректный формат ответа:")
            print(json.dumps(data, indent=4, ensure_ascii=False))
            break

        for card in data['cards']:
            if isinstance(card, dict):
                all_cards.append({
                    key: val for key, val in card.items()
                    if key not in ['description']
                })

        if not data or 'cards' not in data or len(data['cards']) < 100:
            break

        cursor = {
            "updatedAt": data['cursor']['updatedAt'],
            "nmID": data['cursor']['nmID']
        }
        time.sleep(DELAY)

        print(
            f"длина all_cards {len(all_cards)}\nСписок карточек {cabinet} выгружен")

    if all_cards:
        return pd.DataFrame([{'Артикул WB': card['nmID'], 'ID': card['imtID']} for card in all_cards])


def campaing_query(hdrs, status, cabinet, IDKT):
    url_adverts = "https://advert-api.wildberries.ru/adv/v1/promotion/adverts"
    url_fullstats = "https://advert-api.wildberries.ru/adv/v2/fullstats"

    camp_data = []

    date_from = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

    date_to = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    headers = {'Authorization': hdrs}

    payload = {
        'status': status
    }

    adverts = requests.post(url_adverts, headers=headers, params=payload)
    print(f"{cabinet} подключение к {url_adverts}\n{adverts}")
    if adverts.status_code == 204:
        print(f"Нет данных (204) для {cabinet}, статус {status}")
        return pd.DataFrame()

    if adverts.status_code == 200:
        print(f"Статус adverts {adverts}")
        camp = adverts.json()

        print(f"id РК:{[c['advertId'] for c in camp]}")

        params = [{'id': c['advertId'], 'interval': {
            'begin': date_from, 'end': date_to}} for c in camp]

    fullstats = requests.post(url_fullstats, headers=headers, json=params)

    if fullstats.status_code == 200:
        print(f"Статус  {fullstats}")
        full_stats = fullstats.json()

        if full_stats != None:

            for c in full_stats:
                for d in c['days']:
                    for a in d['apps']:
                        for nm in a['nm']:
                            nm['appType'] = a['appType']
                            nm['date'] = d['date']
                            nm['advertId'] = c['advertId']
                            camp_data.append(nm)

            camp_df = pd.DataFrame(camp_data)

            camp_df['date'] = pd.to_datetime(camp_df['date']).dt.date

            camp_df['week'] = pd.to_datetime(
                camp_df['date']).dt.isocalendar().week

            camp_df['status'] = status

            camp_df['Кабинет'] = cabinet

            camp_df = camp_df.rename(columns={
                'sum': 'expenses'
            })
            camp_df = camp_df.drop(columns=['date'])

            group_df = camp_df.groupby([
                'nmId', 'name', 'Кабинет', 'status', 'week'
            ]).agg({
                'views': 'sum',
                'clicks': 'sum',
                'atbs': 'sum',
                'orders': 'sum',
                'shks': 'sum',
                'sum_price': 'sum',
                'expenses': 'sum'

            }).reset_index()

            group_df = group_df.rename(columns={

                'nmId': 'Артикул WB',
                'name': 'Предмет',
                'status': 'Статус РК',
                'week': 'Неделя',
                'views': 'Просмотры',
                'clicks': 'Переходы',
                'atbs': 'Добавления в корзину',
                'orders': 'Количество заказов',
                'shks': 'Количество заказанных товаров',
                'sum_price': 'Сумма заказов',
                'expenses': 'Расход,Р',

            })

            group_df['CTR'] = np.where(
                group_df['Просмотры'] == 0,
                0,
                (
                    group_df['Переходы'] / group_df['Просмотры'] * 100
                ).round(2)
            )

            group_df['CR'] = np.where(
                group_df['Переходы'] == 0,
                0,
                (
                    group_df['Количество заказов'] / group_df['Переходы'] * 100
                ).round(2)
            )

            group_df['CPC'] = np.where(
                group_df['Переходы'] == 0,
                0,
                (
                    group_df['Расход,Р'] / group_df['Переходы']
                ).round(2)
            )
            group_df = group_df.drop(columns=[
                'appType'
            ], errors='ignore')

            result = pd.merge(
                left=group_df,
                right=IDKT,
                left_on='Артикул WB',
                right_on='Артикул WB',
                how='left',
                suffixes=('_Au', '_id'),


            )

            num_col = [
                'Просмотры', 'Переходы', 'Добавления в корзину', 'Количество заказов',
                'Количество заказанных товаров', 'Сумма заказов', 'Расход,Р', 'CTR', 'CR', 'CPC'

            ]

            result = result.reindex(
                columns=['ID'] + result.columns.drop(['ID']).tolist())

            result['Артикул WB'] = result['Артикул WB'].astype(int)

            result['ID'] = result['ID'].astype(int)

            result['Предмет'] = result['Предмет'].replace('', '-').fillna('-')

            for col in num_col:
                result[col] = result[col].fillna(0)

            print(f'✅ Кампания {status} {cabinet} сохранена!')

            result = result.filter([
                'ID', 'Неделя', 'Расход,Р'
            ])

            print(result.head(10))
            return result

        else:
            print(
                f"Ошибка кабинета {cabinet} статус {status} res {print(adverts)}")


def save_to_gh(dict_data):
    GROUP_MAP = {
        'Фин модель Иосифовы Р А М': ['Азарья', 'Рахель', 'Михаил'],
        'Фин модель Галилова': ['Галилова'],
        'Фин модель Мартыненко и Торгмаксимум': ['Мартыненко', 'Торгмаксимум']
    }

    def goup_by_sheet(data, MAP):
        result = defaultdict(pd.DataFrame)

        for name, df in data.items():
            for sheet, people in MAP.items():
                if name in people:
                    result[sheet] = pd.concat(
                        [result[sheet], df], ignore_index=True)

        print('🎉 Данные сгруппированы!')
        return result

    def update_sheet(sheet_df, sheet_name='API WB РК'):
        gs = gspread.service_account(
            filename=r'key.json')

        for name, df in sheet_df.items():
            sh = gs.open(name)
            worksheet = sh.worksheet(sheet_name)

            worksheet.update(
                range_name=f"A{len(worksheet.get_all_values())+1}",
                values=df.values.tolist()
            )
        print('✅ данные выгружены в гугл таблицу!')

    grouped = goup_by_sheet(dict_data, MAP=GROUP_MAP)
    update_sheet(grouped)


if __name__ == '__main__':

    campaing_data = {}

    begin = time.time()
    all_iosifovy = {
        'Азарья': os.getenv('Azarya').strip(),
        'Михаил': os.getenv('Michael').strip(),
        'Рахель': os.getenv('Rachel').strip(),
        'Галилова': os.getenv('Galilova').strip(),
        'Торгмаксимум': os.getenv('TORGMAKSIMUM').strip(),
        'Мартыненко': os.getenv('Martynenko').strip()
    }

    for name, val in all_iosifovy.items():
        idkt = query_idkt(cabinet=name, hdrs=val)
        df_9 = campaing_query(hdrs=val, status=9, cabinet=name, IDKT=idkt)
        time.sleep(60)  # пауза, чтобы не превысить лимит запросов
        df_11 = campaing_query(hdrs=val, status=11, cabinet=name, IDKT=idkt)

        goup = pd.concat([
            df_9, df_11
        ], ignore_index=False).drop_duplicates()

        campaing_data[name] = goup.groupby([
            'ID', 'Неделя',
        ])['Расход,Р'].sum().reset_index()

    print(*[f"Проверка {name}: {len(df)} строк" for name,
          df in campaing_data.items()], sep='\n')

    save_to_gh(campaing_data)

    end = time.time()
    print(f"Время выполнения программы:\n{(end-begin)/60:.2f} минут")

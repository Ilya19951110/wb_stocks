from collections import defaultdict
import numpy as np
import time
import requests
import json
from dotenv import load_dotenv
from datetime import datetime, timedelta
import os
import pandas as pd
import gspread
import openpyxl
from openpyxl.utils.dataframe import dataframe_to_rows
url = "https://advert-api.wildberries.ru/adv/v1/promotion/adverts"
url2 = "https://advert-api.wildberries.ru/adv/v2/fullstats"

# Выгрузка из аурум СТАТИСТИКА РК


def campaing_query(hdrs, status, cabinet):
    idkt = pd.read_csv(f'data/IDKT-{cabinet}.csv')
    camp_data = []

    date_from = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

    date_to = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    headers = {'Authorization': hdrs}

    payload = {
        'status': status
    }

    adverts = requests.post(url, headers=headers, params=payload)
    print(f"{cabinet} подключение к {url}\n{adverts}")
    if adverts.status_code == 204:
        print(f"Нет данных (204) для {cabinet}, статус {status}")
        return pd.DataFrame()

    if adverts.status_code == 200:
        print(f"Статус adverts {adverts}")
        camp = adverts.json()

        print(f"id РК:{[c['advertId'] for c in camp]}")

        params = [{'id': c['advertId'], 'interval': {
            'begin': date_from, 'end': date_to}} for c in camp]

    fullstats = requests.post(url2, headers=headers, json=params)

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
                right=idkt,
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
                columns=['ID КТ'] + result.columns.drop(['ID КТ']).tolist())

            result = result.rename(columns={
                'ID КТ': 'ID'
            })
            result['Артикул WB'] = result['Артикул WB'].astype(int)

            result['ID'] = result['ID'].astype(int)

            result['Предмет'] = result['Предмет'].replace('', '-').fillna('-')

            for col in num_col:
                result[col] = result[col].fillna(0)

            print(f'Кампания {status} {cabinet} сохранена!')

            result = result.filter([
                'ID', 'Неделя', 'Расход,Р'
            ])

            print(result.head(10))
            return result

        else:
            print(
                f"Ошибка кабинета {cabinet} статус {status} res {print(adverts)}")


def save_to_gh(dict_data):
    gs = gspread.service_account(
        filename=r'C:\Users\Ilya\OneDrive\Рабочий стол\my_project\myanaliticmp-0617169ebf44.json')

    data = defaultdict(pd.DataFrame)

    sheet = 'API WB РК'

    for name, df in dict_data.items():
        if name in ['Азарья', 'Рахель', 'Михаил']:
            data['Фин модель Иосифовы Р А М'] = pd.concat(
                [data['Фин модель Иосифовы Р А М'], df], ignore_index=True)

        if name in ['Галилова']:
            data['Фин модель Галилова'] = pd.concat(
                [data['Фин модель Галилова'], df])

        if name in ['Мартыненко', 'Торгмаксимум']:
            data['Фин модель Мартыненко и Торгмаксимум'] = pd.concat(
                [data['Фин модель Мартыненко и Торгмаксимум'], df], ignore_index=True)

    for name, df in data.items():
        sh = gs.open(name)
        worksheet = sh.worksheet(sheet)

        worksheet.update(
            range_name=f"A{len(worksheet.get_all_values())+1}",
            values=df.values.tolist()
        )
    print('кабинеты выгружены в гугл таблицу!')


if __name__ == '__main__':

    campaing_data = {}

    begin = time.time()
    all_iosifovy = {
        'Азарья': os.getenv('Azarya'),
        'Михаил': os.getenv('Michael'),
        'Рахель': os.getenv('Rachel'),
        'Галилова': os.getenv('Galilova'),
        'Торгмаксимум': os.getenv('TORGMAKSIMUM'),
        'Мартыненко': os.getenv('Martynenko')
    }

    for name, val in all_iosifovy.items():
        df_9 = campaing_query(hdrs=val, status=9, cabinet=name)
        time.sleep(60)
        df_11 = campaing_query(hdrs=val, status=11, cabinet=name)

        goup = pd.concat([
            df_9, df_11
        ], ignore_index=False).drop_duplicates()

        campaing_data[name] = goup.groupby([
            'ID', 'Неделя',
        ])['Расход,Р'].sum().reset_index()

    for name, df in campaing_data.items():
        print(f"Проверка {name}: {len(df)} строк")

    save_to_gh(campaing_data)

    end = time.time()
    print(f"Время выполнения программы:\n{(end-begin)/60}")

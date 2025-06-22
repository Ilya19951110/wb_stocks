from gspread_dataframe import set_with_dataframe
import json
import requests
import os
import pandas as pd
import gspread
from scripts.gspread_client import get_gspread_client


def tariffs_for_boxes():

    gs = get_gspread_client()
    url = "https://common-api.wildberries.ru/api/v1/tariffs/box"

    spreadsheet = gs.open('Ассортиментная матрица. Полная')
    sheets = spreadsheet.worksheet('API(Тарифы коробов)')

    headers = {
        "Authorization": os.getenv('Rachel').strip()
    }

    params = {
        "date": '2025-01-01'
    }

    res = requests.get(url, headers=headers, params=params)
    result = res.json()

    box = pd.DataFrame(result['response']['data']['warehouseList'])

    box['dtNextBox'] = result['response']['data']['dtNextBox']
    box['dtTillMax'] = result['response']['data']['dtTillMax']

    box = box.rename(columns={
        'boxDeliveryAndStorageExpr': 'Доставка_и_хранение',
        'boxDeliveryBase': 'Доставка_базовая',
        'boxDeliveryLiter': 'Доставка_за_литр',
        'boxStorageBase': 'Хранение_база_день',
        'boxStorageLiter': 'Хранение_за_литр_день',
        'warehouseName': 'Склад'
    })

    target_col = [col for col in box.columns if col not in box.columns[-3:]]

    box[target_col] = box[target_col].replace('-', 0)

    box[target_col] = box[target_col].replace(',', '.', regex=True)

    box[target_col] = box[target_col].apply(
        pd.to_numeric, errors='coerce').fillna(0)

    sheets.batch_clear(['A:H'])
    set_with_dataframe(
        sheets, box, include_column_header=True, include_index=False)


if __name__ == '__main__':
    tariffs_for_boxes()

from scripts.utils.gspread_client import get_gspread_client
from scripts.utils.config.factory import get_requests_url_wb
from scripts.utils.config.factory import sheets_names, tables_names
from gspread_dataframe import set_with_dataframe
from datetime import datetime
from scripts.utils.telegram_logger import send_tg_message
import pandas as pd
import requests
import os


def tariffs_for_boxes(clear_range: list[str] = ['A:H']) -> None:
   
    gs = get_gspread_client()

    spreadsheet = gs.open(tables_names()['wb_matrix_complete'])
    sheets = spreadsheet.worksheet(sheets_names()['tariffs_box_api'])

    headers = {
        "Authorization": os.getenv('Rachel').strip()
    }

    params = {
        "date": datetime.now().strftime('%Y-%m-%d')
    }

    res = requests.get(url=get_requests_url_wb()[
                       'tariffs_box'], headers=headers, params=params)

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
    print(box.columns.tolist(), box.shape, sep='\n')
    target_col = [col for col in box.columns if col not in box.columns[-3:]]

    box[target_col] = box[target_col].replace('-', 0)

    box[target_col] = box[target_col].replace(',', '.', regex=True)

    box[target_col] = box[target_col].apply(
        pd.to_numeric, errors='coerce').fillna(0)

    
    sheets.batch_clear(clear_range)
    sheets.update(
        values=[box.columns.values.tolist()] +
            box.values.tolist(),
        )
   
    send_tg_message(
        "📦 Тарифы WB по коробам успешно обновлены в Google Таблице")


if __name__ == '__main__':

    tariffs_for_boxes()

#    py -m scripts.pipelines.tariffs-for-boxes
#  box = box.rename(columns={
#                 'boxDeliveryAndStorageExpr': 'Доставка и хранение',
#                 'boxDeliveryBase': 'Логистика первый литр,₽',
#                 'boxDeliveryLiter': 'Доставка за литр',
#                 'boxStorageBase': 'Хранение в день,₽',
#                 'boxStorageLiter': 'Хранение в день, доп. литр,₽',
#                 'warehouseName': 'Склад',
#                 'boxDeliveryCoefExpr':'Коэфициент Логистика %, Учтен в тарифах',
#                 'boxDeliveryMarketplaceBase': 'Логистика доп. литр,₽',
#                 'boxDeliveryMarketplaceLiter': 'Коэфициент FBS %. Уже учтен в тарифах',
#                 'boxStorageCoefExpr': 'Коэфициент Хранение %, Учтен в тарифах',
#                 'geoName': 'Страна/Округ',
#                 'dtNextBox': 'Дата начала следующего тарифа',
#                 'dtTillMax': 'Дата окончания последнего утсановленного тарифа'
#             })
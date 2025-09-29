import requests
import json
from scripts.utils.setup_logger import make_logger
from scripts.utils.config.factory import get_requests_url_wb, get_client_info
import pandas as pd
import os
from dotenv import load_dotenv
load_dotenv()

url = get_requests_url_wb()['delete_cards']
logger = make_logger(__name__, use_telegram=False)

def delete_cards(data:dict):
    
    chunk = 1000
    for name, conf in data.items():
        logger.info(f"🏪 Подключаюсь к кабинету: {name}")
        for api, nmid in conf.items():
            headers = {
            'Authorization': api
        }
            chunks = [nmid[i:i+chunk] for i in range(0, len(nmid), chunk)]
            
            for idx, s in enumerate(chunks, start=1):
                params = {'nmIDs': s}

                try:
                    response = requests.post(url=url, headers=headers, json=params)
                    response.raise_for_status()
                    logger.info(f"✅ {name}: чанк {idx}/{len(chunks)} удалён, {len(s)} карточек")
                except requests.RequestException as e:
                    logger.error(f"❌ {name}: ошибка при удалении чанка {idx}/{len(chunks)} → {e}")
    
if __name__ == '__main__':

    ram = get_client_info()['api_keys_wb']


    
    data = pd.read_excel(
        r"C:\Users\Ilya\OneDrive\Рабочий стол\артикулы под удаление.xlsx",
        sheet_name='Лист1',
        index_col=False
    )
    cabinet = data['ИП'].unique()

    print(data.columns.tolist(),cabinet, data.shape, sep='\n')

    res = {
        name: {ram[name]: group['Арт'].tolist()}
        for name, group in data.groupby('ИП')
        if name in ram
    }
    delete_cards(res)
    # C:\Users\Ilya\OneDrive\Рабочий стол\iosifovy\ py -m scripts.pipelines.delete_cards

  


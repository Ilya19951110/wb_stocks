from scripts.spreadsheet_tools.push_mywarehouse import upload_my_werehouse_df_in_assortment_matrix_full
from scripts.utils.setup_logger import make_logger
from dotenv import load_dotenv
import pandas as pd
import requests
import time
import os
load_dotenv()


logger = make_logger(__name__, use_telegram=True)


def get_mywerehouse_stocks() -> pd.DataFrame:
    logger.info("📦 Старт сбора остатков из API Мой Склад")

    url = 'https://api.moysklad.ru/api/remap/1.2/report/stock/all'
    headers = {
        'Authorization': f"Bearer {os.getenv('my_warehouse')}",

    }
    print("🧪 my_warehouse =", os.getenv('my_warehouse'))

    werehouse = []
    offset = 0
    limit = 1000
    while True:
        try:

            params = {
                'limit': limit,
                'offset': offset
            }
            logger.info(f"🔄 Получение с offset= {offset}")

            res = requests.get(url=url, headers=headers, params=params)

            if res.status_code != 200:
                logger.warning(f"⚠️ Ошибка запроса: {res.status_code}")
                logger.warning(res.text)
                break

            result = res.json()
            rows = result.get('rows', [])

            if not rows:
                logger.info("🚫 Больше данных нет, завершаем.")
                break

            werehouse.extend(rows)

            logger.info(
                f"✅ Загружено {len(rows)} строк, всего: {len(werehouse)}")

            offset += limit

            time.sleep(0.5)

        except Exception:
            logger.exception("❌ Ошибка при получении данных с API")

    result_werehouse = []
    for house in werehouse:
        result_werehouse.append({
            # 'meta': house.get('meta'),
            'ОстаткиВсего': house.get('stock'),
            'inTransit': house.get('inTransit'),
            'reserve': house.get('reserve'),
            'Остатки': house.get('quantity'),
            'Наименование': house.get('name'),
            'code': house.get('code'),
            'АртикулМойСклад': house.get('article'),
            'price': house.get('price'),
            'salePrice': house.get('salePrice'),
            'uom': house.get('uom', {}).get('name', 'Не указано'),
            'externalCode': house.get('externalCode'),
            'stockDays': house.get('stockDays'),
            "cabinet": house.get('folder', {}).get("name")
        })
    logger.info(f"📊 Получено {len(result_werehouse)} позиций от МойСклад")

    res = pd.DataFrame(result_werehouse)

    filtered_res = res.filter(
        ['АртикулМойСклад', 'ОстаткиВсего'])

    logger.info(filtered_res.head(2))
    return filtered_res


if __name__ == '__main__':
    #  Запуск

    werehouse = get_mywerehouse_stocks()

    upload_my_werehouse_df_in_assortment_matrix_full(
        mywerehouse=werehouse, clear_range=['A:B'])
# py -m scripts.my_werehouse.get_warehouse_api

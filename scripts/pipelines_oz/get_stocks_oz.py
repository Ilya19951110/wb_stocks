import psutil

import pandas as pd
import os
import asyncio
import aiohttp
import time
from scripts.utils.setup_logger import make_logger
from scripts.utils.telegram_logger import send_tg_message
from scripts.utils.config.factory import get_requests_url_oz
from dotenv import load_dotenv
from scripts.engine.main_ozon import main_run_ozon
from scripts.engine.run_cabinet_oz import execute_run_ozon
import io
from scripts.utils.config.factory import get_headers
from scripts.spreadsheet_tools.upload_oz_matrix_gsheet import upload_oz_stocks_oz_matrix
from scripts.utils.config.factory import get_all_api_ozon
load_dotenv()

logger = make_logger(__name__, use_telegram=True)


# def get_memory_usage_mb() -> float:
#     """Возвращает текущую память процесса в мегабайтах"""
#     process = psutil.Process(os.getpid())
#     mem_bytes = process.memory_info().rss  # Resident Set Size (RSS)
#     return mem_bytes / (1024 ** 2)


# async def get_report_products_create(api_key: str, client_id: str, name: str, sessions: aiohttp.ClientSession) -> str:

#     try:
#         logger.info('📤 Отправляю запрос на создание отчета SELLER_PRODUCTS')
#         url = get_requests_url_oz()['product_create']

#         page = 1

#         params = {
#             'page': page,
#             'page_size': 1000,
#             'report_type': 'SELLER_PRODUCTS',
#         }

#         async with sessions.post(url, headers=get_headers(api_key=api_key, client_id=client_id), json=params) as response:

#             if response.status != 200:
#                 logger.error(
#                     f'КАБИНЕТ {name} - ❌ Ошибка при создании отчета: {await response.text()}')
#                 raise RuntimeError("Не удалось создать отчет")

#             result = await response.json()
#             logger.info(
#                 f"КАБИНЕТ {name} - ✅ Отчет создан, код: {result['result']['code']}")

#         return result['result']['code']

#     except Exception:
#         logger.exception(
#             f'КАБИНЕТ {name} - 💥 Ошибка в get_report_products_create')
#         raise


# async def get_report_info(api_key: str, client_id: str, code: str, name: str, sessions: aiohttp.ClientSession, max_retries: int = 20, delay: int = 10,) -> str:
#     """
#     Ждёт готовности отчета Ozon и возвращает ссылку на файл, когда он будет готов.

#     Args:
#         code: код отчета
#         max_retries: сколько раз пробовать
#         delay: пауза между попытками (сек)

#     Returns:
#         str: ссылка на CSV-файл
#     """
#     url = get_requests_url_oz()['report_info']

#     for attempt in range(1, max_retries + 1):

#         try:
#             logger.info(
#                 f"⏳ [{attempt}] Проверяю готовность отчета с кодом {code}")

#             async with sessions.post(url, headers=get_headers(api_key=api_key, client_id=client_id), json={'code': code}) as response:

#                 response.raise_for_status()

#                 result = await response.json()
#                 status = result['result'].get('status', '')

#                 if status == 'success':
#                     logger.warning(
#                         f"✅ [{attempt}] Отчет готов! Получаем ссылку на файл.")

#                     return result['result']['file']

#                 elif status == 'failed':
#                     logger.error(
#                         f"КАБИНЕТ {name} - ❌ Отчет завершился неудачей. Код: {code}")
#                     raise RuntimeError("Отчет завершился с ошибкой")

#                 logger.warning(
#                     f"КАБИНЕТ {name} - ⌛ [{attempt}] Отчет в статусе: {status}. Повтор через {delay} сек.")
#                 await asyncio.sleep(delay)

#         except Exception:
#             logger.exception(
#                 f"КАБИНЕТ {name} - 💥 Ошибка при проверке статуса отчета. Попытка {attempt}")
#             await asyncio.sleep(delay)

#     raise TimeoutError(f"КАБИНЕТ {name} - ⛔ Отчет не был готов вовремя")


# async def get_csv_report(file: str, name: str, sessions: aiohttp.ClientSession) -> tuple[pd.DataFrame, pd.Series]:

#     try:
#         logger.info(f"КАБИНЕТ {name} - 📥 Загружаю CSV-отчет из URL: {file}")

#         async with sessions.get(file) as response:

#             response.raise_for_status()

#             content = await response.read()
#             buffer = io.BytesIO(content)

#             logger.info(
#                 f"КАБИНЕТ {name} - 📦 Перед загрузкой CSV: {get_memory_usage_mb():.2f} MB")

#             df_info = pd.read_csv(buffer, sep=';', encoding='utf-8')

#             try:

#                 logger.warning('сохраняю sku в xl')

#                 df_info.to_excel(
#                     r"C:\Users\Ilya\OneDrive\Рабочий стол\skuV2.xlsx",
#                     index=False,
#                     engine='openpyxl',
#                     sheet_name='SKU_test'
#                 )
#                 logger.warning('sku в xl +++')
#             except Exception:
#                 logger.exception(f'ошибка сохраняния в xl {name}')

#             logger.info(
#                 f"КАБИНЕТ {name} - ✅ CSV успешно загружен, строк: {df_info.shape[0]}")
#             logger.warning(
#                 f"КАБИНЕТ {name} - 📦 После загрузки CSV: {get_memory_usage_mb():.2f} MB")

#             sku = df_info['SKU']
#         return df_info, sku

#     except Exception:
#         logger.exception(
#             f"КАБИНЕТ {name} - 💥 Ошибка при загрузке или чтении CSV-файла")
#         raise


async def get_product_list_stocks(api_key: str, client_id: str, sku: list[int], name: str, sessions: aiohttp.ClientSession) -> pd.DataFrame:
    all_stocks = []

    url = get_requests_url_oz()['analytics_stocks']

    chunk = 100

    for i in range(0, len(sku), chunk):

        try:
            logger.info(
                f"КАБИНЕТ {name} 🔍 Начинаю получение аналитики по {len(sku)} SKU")
            chunk_size = sku[i:i+chunk]

            params = {

                'skus': list(map(str, chunk_size))
            }
            async with sessions.post(url, headers=get_headers(client_id=client_id, api_key=api_key), json=params) as response:

                if response.status != 200:
                    logger.error(
                        f"КАБИНЕТ {name} ❌ Ошибка запроса {url}: {response.status} — {await response.text()}")

                result = await response.json()

                all_stocks.extend(result['items'])
                logger.info(
                    f"КАБИНЕТ {name} 📊 Текущий объем: {len(all_stocks)} записей")

                await asyncio.sleep(3)

        except Exception:
            logger.exception(
                f"КАБИНЕТ {name} 💥 Ошибка при получении данных по SKU {i+1}–{i+len(chunk_size)}")

    logger.info(
        f"КАБИНЕТ {name} ✅ Получено всего: {len(all_stocks)} записей по складам")

    return pd.DataFrame(all_stocks)

if __name__ == '__main__':
    start = time.perf_counter()
    send_tg_message("🚀 Старт скрипта 'get_skus_ozon'")
    # python -m scripts.pipelines_oz.get_stocks_oz

    data_stocks = asyncio.run(main_run_ozon(
        run_func=execute_run_ozon,
        cabinet_oz=get_all_api_ozon()

    ))

    upload_oz_stocks_oz_matrix(data=data_stocks)
    end = time.perf_counter()
    logger.warning(f" Время выполнения: {(end-start)/60:,.2f} мин.")

from scripts.utils.setup_logger import make_logger
from scripts.utils.config.factory import get_all_api_request
from dotenv import load_dotenv
from collections import defaultdict
import aiohttp
import asyncio
import pickle
import os

load_dotenv()
# главная функция асинхронных запросов
logger = make_logger(__name__, use_telegram=True)


async def main(run_funck, postprocess_func=None, cabinet=None, cache_name="test_cache.pkl"):

    status_report = defaultdict(str)
    result, failed = {}, {}

    if cabinet is None:
        all_api_request = get_all_api_request()

    else:
        all_api_request = cabinet

    async with aiohttp.ClientSession() as session:
        tasks = [
            run_funck(name=name, api=api, session=session)
            for name, api in all_api_request.items()
        ]

        response = await asyncio.gather(*tasks, return_exceptions=True)

        for (name, api_key), res in zip(all_api_request.items(), response):

            if isinstance(res, Exception):
                logger.error(f"❌ Ошибка в {name}: {res}")

                status_report[name] = "❌ ОШИБКА"
                failed[name] = api_key
                continue

            if postprocess_func:
                try:
                    if res is not None and isinstance(res, (list, tuple)):
                        data = postprocess_func(*res, name=name)

                        result[name] = data
                        logger.info(f'🔥🔥🔥🔥🔥\nЗапрос {name} успешно выполнен!!')

                        status_report[name] = "✅ УСПЕШНО"
                    else:
                        logger.warning(
                            f"⚠️ Пропущен postprocess для {name} — res = {res}")

                        status_report[name] = "⚠️ ПРОПУЩЕН"
                        failed[name] = api_key

                except Exception as e:
                    logger.error(
                        f"❌ Ошибка при postprocess {postprocess_func.__name__} {name} - res: {e}")
            else:
                result[name] = res
                logger.warning(f'🔥🔥🔥🔥🔥\nЗапрос  {name} НЕ выполнен!!')

                status_report[name] = "⚠️ НЕ ОБРАБОТАН POSTPROCESS"
                failed[name] = api_key

    logger.info("📊 ИТОГОВЫЙ ОТЧЁТ ПО КАБИНЕТАМ:")
    for name, status in status_report.items():
        logger.info(f"{name:<15} - {status}")

    return result

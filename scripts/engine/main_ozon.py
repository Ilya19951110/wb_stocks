import os
import asyncio
import aiohttp
from scripts.utils.setup_logger import make_logger
import pandas as pd
from scripts.utils.telegram_logger import send_tg_message

logger = make_logger(__name__, use_telegram=False)


async def main_run_ozon(run_func, cabinet_oz: dict[str, dict[str, str]]) -> dict[str, pd.DataFrame]:
    """
   Универсальный асинхронный запуск для кабинетов OZON с передачей headers (dict).
   """

    final_report = {}
    result = {}

    async with aiohttp.ClientSession() as sessions:
        tasks = [
            run_func(
                api_key=api_data['Api-Key'], client_id=api_data['Client-Id'], name=name, sessions=sessions)

            for name, api_data in cabinet_oz.items()
        ]

        response = await asyncio.gather(*tasks, return_exceptions=True)

        for (name, _), res in zip(cabinet_oz.items(), response):
            if isinstance(res, Exception):
                final_report[name] = '❌ Ошибка'
                logger.error(f"❌ Ошибка в {name}: {res}")

            else:
                result[name] = res
                final_report[name] = f'✅ {name} выполнен успешно'

    for name, report in final_report.items():
        send_tg_message(f"{name}: {report}")

    for name, group_df in result.items():
        logger.warning(f"{name} -> {group_df.shape}")

    logger.info("🏁 Все кабинеты обработаны")
    send_tg_message("🏁 Обработка всех кабинетов завершена")

    return result

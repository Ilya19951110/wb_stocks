import asyncio
import aiohttp
from scripts.utils.setup_logger import make_logger
import pandas as pd
from scripts.utils.telegram_logger import send_tg_message

logger = make_logger(__name__, use_telegram=False)


async def main_run_ozon(run_func, cabinet_oz: dict[str, dict[str, str]]) -> dict[str, pd.DataFrame]:
    """
    🔄 Асинхронный универсальный запуск обработки кабинетов OZON.

    Функция запускает асинхронные задачи для каждого кабинета OZON, передавая авторизационные заголовки
    (`api_key` и `client_id`) в указанную функцию `run_func`. Полученные результаты (обычно `pd.DataFrame`)
    собираются в единый словарь, а статус выполнения каждого кабинета логируется и отправляется в Telegram.

    ─────────────────────────────────────────────────────────────

    🔧 Параметры:
    -------------
    run_func : Callable
        Асинхронная функция, которая будет вызвана для каждого кабинета. Должна принимать аргументы:
        - `api_key: str`
        - `client_id: str`
        - `name: str`
        - `sessions: aiohttp.ClientSession`

    cabinet_oz : dict[str, dict[str, str]]
        Словарь кабинетов OZON в формате:
        {
            "Название кабинета": {
                "Api-Key": "...",
                "Client-Id": "..."
            },
            ...
        }

    📤 Возвращает:
    --------------
    dict[str, pd.DataFrame]
        Словарь, где ключ — название кабинета, значение — DataFrame с результатами, полученными от `run_func`.

    📬 Telegram уведомления:
    -------------------------
    - После обработки каждого кабинета: статус (успех / ошибка).
    - После завершения всех кабинетов: сообщение об окончании.
    - Для успешных — логируется `.shape` полученного DataFrame.

    ⚠️ Ошибки:
    ----------
    - Если функция для кабинета выбрасывает исключение — записывается в лог и в Telegram,
      результат по такому кабинету не возвращается.

    🧠 Автор: Илья  
    🗓 Версия: Июль 2025
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
                msg = f"❌ Ошибка в {name}: {res}"

                logger.error(msg)

            else:
                result[name] = res
                final_report[name] = f'✅ {name} выполнен успешно'

    for name, report in final_report.items():
        send_tg_message(f"{name}: {report}")

    for name, group_df in result.items():
        send_tg_message(f"{name} -> {group_df.shape}")

    logger.info("🏁 Все кабинеты обработаны")
    send_tg_message("🏁 Обработка всех кабинетов завершена")

    return result

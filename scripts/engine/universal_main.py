from scripts.utils.setup_logger import make_logger
from scripts.utils.telegram_logger import send_tg_message
from scripts.utils.config.factory import get_client_info
from dotenv import load_dotenv
from collections import defaultdict
import aiohttp
import asyncio
import pandas as pd

load_dotenv()

logger = make_logger(__name__, use_telegram=False)


async def main(run_funck, exclude_names: list[str] = None, postprocess_func=None, cabinet=None) -> dict[str, tuple[pd.DataFrame, pd.DataFrame]]:
    """
    🔁 Универсальный асинхронный движок для запуска обработки по кабинетам WB/Ozon.

    Функция `main` выполняет асинхронный обход всех кабинетов (или переданных вручную через `cabinet`),
    вызывает функцию `run_funck`, а затем (опционально) применяет к результату `postprocess_func`. 
    Возвращает итоговый словарь `{кабинет: (DataFrame, DataFrame)}`.

    ─────────────────────────────────────────────────────────────

    🔧 Аргументы:
    ------------
    run_funck : Callable[..., Awaitable]
        Асинхронная функция, вызываемая для каждого кабинета. 
        Должна принимать аргументы `name`, `api`, `session` и возвращать результат (обычно кортеж или DataFrame).

    postprocess_func : Callable[..., tuple[pd.DataFrame, pd.DataFrame]], optional
        Функция постобработки, применяемая к результату `run_funck` (например, объединение данных, агрегации и пр.).
        Принимает распакованный результат `*res`, а также `name` как именованный аргумент.

    cabinet : dict[str, str], optional
        Словарь кабинетов в формате `{name: api_key}`. Если не указан — используется `get_client_info()`.

    📤 Возвращает:
    --------------
    dict[str, tuple[pd.DataFrame, pd.DataFrame]]
        Словарь с результатами обработки для каждого кабинета.
        Ключ — имя кабинета, значение — кортеж с двумя DataFrame.

    ⚠️ Обработка ошибок:
    ---------------------
    - Если `run_funck` завершился ошибкой, результат сохраняется в `failed`.
    - Ошибки постобработки логируются и отправляются в Telegram.
    - Telegram получает сводный статус по каждому кабинету.

    📈 Пример использования:
    ------------------------
    data = asyncio.run(main(
        run_funck=execute_run_cabinet,
        postprocess_func=group_advert_and_id,
    ))

    🧠 Автор: Илья  
    🗓 Версия: Июль 2025
    """
    status_report = defaultdict(str)
    result, failed = {}, {}

    exclude_names = exclude_names or []

    if exclude_names:
        logger.info(f"⛔ Исключены из обработки: {', '.join(exclude_names)}")

    if cabinet is None:
        all_api_request = get_client_info()['api_keys_wb']

    else:
        all_api_request = cabinet

    all_api_request = {
        name: api for name, api in all_api_request.items() if name not in exclude_names
    }

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
                    msg = f"❌ Ошибка при postprocess {postprocess_func.__name__} {name} - res: {e}"
                    logger.error(msg)
                    send_tg_message(msg)
            else:
                result[name] = res
                logger.warning(f'🔥🔥🔥🔥🔥\nЗапрос  {name} НЕ выполнен!!')

                status_report[name] = "⚠️ НЕ ОБРАБОТАН POSTPROCESS"
                failed[name] = api_key

    send_tg_message("📊 ИТОГОВЫЙ ОТЧЁТ ПО КАБИНЕТАМ:")
    for name, status in status_report.items():
        send_tg_message(f"{name:<15} - {status}")

    return result

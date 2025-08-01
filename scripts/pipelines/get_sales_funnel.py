from scripts.spreadsheet_tools.upload_to_gsheet_advert_sales import save_in_gsh
from scripts.postprocessors.group_sales import get_current_week_sales_df
from scripts.utils.config.factory import get_requests_url_wb, sheets_names
from scripts.engine.run_cabinet import execute_run_cabinet
from scripts.utils.telegram_logger import send_tg_message
from scripts.utils.setup_logger import make_logger
from scripts.utils.telegram_logger import send_tg_message
from scripts.engine.universal_main import main
from dotenv import load_dotenv
from datetime import datetime, timedelta
from functools import partial
import aiohttp
import asyncio
import time


load_dotenv()

logger = make_logger(__name__, use_telegram=False)


async def report_detail(name: str, api: str, session: aiohttp.ClientSession):
    """
    📊 Скрипт сбора детализированной статистики продаж с Wildberries API (report_detail) и сохранения её в Google Sheets

    Этот модуль предназначен для автоматической выгрузки карточной статистики с кабинетов Wildberries
    (по API), последующей агрегации данных по неделям и выгрузке в таблицу Google Sheets.

    Связан с функцией `save_in_gsh`, которая сохраняет агрегированные данные по кабинетам
    в разные итоговые Google Таблицы в зависимости от маппинга.

    ───────────────────────────────────────────────────────────────────────────────

    🔄 Основной процесс:

    1. Запускается асинхронный сбор статистики карточек по кабинетам Wildberries с помощью функции `report_detail`.
    2. Каждый кабинет (определяется через `execute_run_cabinet`) обрабатывается независимо.
    3. Полученные "сырые" данные передаются в функцию `get_current_week_sales_df`, где:
        - нормализуются вложенные JSON-объекты,
        - считается номер недели (`Неделя`) по дате,
        - данные агрегируются по метрикам и группируются по `ID` и `Неделя`.
    4. Результаты сохраняются в Google Sheets с помощью `save_in_gsh`.

    ───────────────────────────────────────────────────────────────────────────────

    🔧 Параметры:

    - `name` (str): название кабинета (бренд, аккаунт и т.п.)
    - `api` (str): API-ключ авторизации Wildberries.
    - `session` (aiohttp.ClientSession): сессия для асинхронных HTTP-запросов.

    📦 Возвращает:
        list[dict]: список карточек (данные за последние 7 дней), объединённых по страницам.

    ───────────────────────────────────────────────────────────────────────────────

    📤 Итоговая выгрузка:
    - Таблица: определяется по ключу `api_wb_sales` из `sheets_names()`
    - Лист: `worksheet_name` передаётся в `save_in_gsh`

    ───────────────────────────────────────────────────────────────────────────────

    📅 Период отчёта:
    - Считается от текущей даты: последние **7 дней**, не включая сегодняшний.

    🕒 Лимит API:
    - Пагинация по 1000 карточек
    - Пауза 21 секунда между запросами

    ───────────────────────────────────────────────────────────────────────────────

    🧩 Зависимости:
    - `aiohttp`, `asyncio`
    - `scripts.engine.run_cabinet.main`
    - `get_current_week_sales_df` — агрегация и постобработка
    - `save_in_gsh` — сохранение результатов
    - `sheets_names`, `get_requests_url_wb`, `dotenv`, `gspread`, `pandas`

    ───────────────────────────────────────────────────────────────────────────────

    🧑‍💻 Автор: Илья  
    📅 Версия: Июль 2025

    """
    stop, page, all_data = 21, 1, []
    url = get_requests_url_wb()
    while True:

        headers = {
            'Authorization': api,
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        params = {

            'timezone': 'Europe/Moscow',
            'period': {

                'begin': (datetime.now()-timedelta(days=7)).strftime('%Y-%m-%d 00:00:00'),
                'end': (datetime.now()-timedelta(days=1)).strftime('%Y-%m-%d 23:59:59')
            },
            'orderBy': {
                'field': 'openCard',
                'mode': 'desc'
            },
            'page': page
        }
        try:
            async with session.post(url['report_detail'], headers=headers, json=params) as result:
                logger.info(f"{name} подключение {result.status}")

                if result.status != 200:
                    logger.error(f"Ошибка запроса: {result.status}")
                    send_tg_message(f'report_detail_sales:\n{result.status}')
                    break

                detail = await result.json()
        except Exception as e:
            msg = f"❌❌  Произошла ошибка при запросе к {name}: {e}"
            send_tg_message(msg)
            logger.error(msg)

        else:
            cards = detail.get('data', {}).get('cards', [])

            if not cards:
                break

            all_data.extend(cards)

            logger.info(
                f'Получено {len(cards)} записей кабинета {name}. Всего: {len(all_data)}')
            if len(cards) < 1000:
                break

            page += 1
            logger.info(f'Спим {stop} сек')

            await asyncio.sleep(stop)

    return all_data


if __name__ == '__main__':

    send_tg_message(
        f"🏁 Скрипт запущен 'report_detail': {datetime.now():%Y-%m-%d %H:%M:%S}")
    begin = time.time()

    data = asyncio.run(main(
        run_funck=partial(execute_run_cabinet,
                          func_name='report_detail'),
        postprocess_func=get_current_week_sales_df,

    ))

    worksheet = sheets_names()['api_wb_sales']
    save_in_gsh(dict_data=data, worksheet_name=worksheet)

    end = time.time()
    logger.info(f"⏱ Выполнено за {(end - begin)/60:.2f} минут")
# py -m scripts.pipelines.get_sales_funnel

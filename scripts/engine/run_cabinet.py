
from scripts.pipelines.get_cards_list import get_cards
from scripts.utils.setup_logger import make_logger
from dotenv import load_dotenv
from typing import Optional
import pandas as pd
import aiohttp


load_dotenv()
# Проверяем пустой ли дата фрейм
logger = make_logger(__name__)


async def execute_run_cabinet(name: str, api: str,
                              session: aiohttp.ClientSession,
                              func_name: Optional[str] = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    🧠 Универсальный асинхронный запуск функций по кабинетам (WB)

    Эта функция служит маршрутизатором для вызова одной из трёх функций аналитики:
    - Получение остатков (`get_stocks`)
    - Отчёт по воронке продаж (`report_detail`)
    - Статистика рекламных кампаний (`campaign_query`)

    Она:
    ▪ валидирует `func_name`
    ▪ получает карточки товаров (ID/IDKT) через `get_cards()`
    ▪ вызывает нужную функцию и обрабатывает результат

    Параметры:
        name (str): Название кабинета (например, 'Галилова', 'Азаря')
        api (Any): Авторизованный API-объект клиента (например, aiohttp.ClientSession)
        session (Any): Сессия подключения к WB/Ozon API
        func_name (str): Название вызываемой функции (`get_stocks`, `report_detail`, `campaign_query`)

    Возвращает:
        tuple[pd.DataFrame, pd.DataFrame] или None:
            - Для get_stocks → (результат, IDKT)
            - Для остальных  → (результат, ID)
            - В случае ошибки или пустых карточек → (empty DataFrames) или None

    Исключения:
        ValueError: если `func_name` не поддерживается
        Любые исключения из вызываемых функций логируются и подавляются (возвращается None)

    Зависимости:
        - scripts.get_cards_list.get_cards
        - scripts.get_sales_funnel.report_detail
        - scripts.get_advertising_report.campaign_query
        - scripts.get_supplier_stocks.get_stocks

    📌 Пример использования:
        result, ids = await execute_run_cabinet("Галилова", api=api, session=session, func_name="get_stocks")
    """

    logger.debug(f"🧪 DEBUG: FUNC_NAME = {func_name.upper()}")

    from scripts.pipelines.get_sales_funnel import report_detail
    from scripts.pipelines.get_advertising_report import campaign_query
    from scripts.pipelines.get_supplier_stocks import get_stocks

    allowed = {
        'get_stocks': get_stocks,
        'report_detail': report_detail,
        'campaign_query': campaign_query
    }

    if func_name not in allowed:
        message = f"❌ func_name '{func_name}' не поддерживается. Доступные: {list(allowed.keys())}"
        logger.error(message)
        raise ValueError(message)

    try:
        logger.info(f"🚀 Запускаю get_cards для: {name}")
        IDKT, ID = await get_cards(name=name, api=api, session=session)

        if IDKT.empty or ID.empty:
            logger.warning(f"IDKT - пустой!!")
            return pd.DataFrame(), pd.DataFrame()

        logger.info(
            f"✅ get_cards завершён: {name}, {len(ID)} карточек получено")

    except Exception as e:
        logger.error(f"Ошибка запроса IDKT кабинета: {name}: {e}")
        return pd.DataFrame(), pd.DataFrame()

    func = allowed[func_name.lower()]

    try:
        logger.debug(
            f"🟢 Запускаю `{func_name}` для кабинета `{name}`")

        logger.debug(f"🧪 DEBUG: SELECTED FUNCTION = {func.__name__.upper()}")

        result = await func(name=name, api=api, session=session)

        logger.info(f"✅ `{func_name}` успешно отработала для `{name}`")
        logger.info(f"🏁 Кабинет `{name}` завершён без ошибок")

        if func_name == 'get_stocks':
            return result, IDKT

        else:
            return result, ID

    except Exception as e:
        logger.error(f"❌ Ошибка в `{func_name}` для `{name}`: {e}")
        return pd.DataFrame(), pd.DataFrame()

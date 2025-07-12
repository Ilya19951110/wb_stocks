from scripts.utils.setup_logger import make_logger
import aiohttp
import pandas as pd

logger = make_logger(__name__, use_telegram=True)


async def execute_run_ozon(api_key: str, client_id: str, name: str, sessions: aiohttp.ClientSession) -> pd.DataFrame:
    """
    Запускает сбор и агрегацию отчёта по остаткам и карточкам товаров для заданного кабинета Ozon.

    Функция выполняет последовательный асинхронный сбор данных по API Ozon:
    1. Загружает атрибуты товаров через `get_product_info_attributes`.
    2. Извлекает список SKU с помощью `extract_sku`.
    3. Получает текущие остатки товаров через `get_product_list_stocks`.
    4. Читает сохранённые атрибуты и объединяет данные с остатками.
    5. Обрабатывает данные функцией `prepare_final_ozon_data` и возвращает итоговый DataFrame.

    Используется в рамках сбора аналитики по кабинетам Ozon с сохранением асинхронности (через aiohttp сессии).

    Args:
        api_key (str): API-ключ кабинета Ozon.
        client_id (str): Идентификатор клиента (cabinet ID).
        name (str): Название кабинета (используется как ключ или псевдоним).
        sessions (aiohttp.ClientSession): Общая aiohttp-сессия для запросов.

    Returns:
        pd.DataFrame: Объединённый и обработанный DataFrame с остатками и характеристиками товаров.

    Raises:
        Exception: При ошибках во время сбора данных (например, отказ API или некорректный ответ).
    """
    from scripts.postprocessors.ozon_data_transform import prepare_final_ozon_data
    from scripts.pipelines_oz.get_cards_list_oz import extract_sku, get_product_info_attributes, read_product_info_json
    from scripts.pipelines_oz.get_stocks_oz import get_product_list_stocks
    # get_report_products_create, get_report_info, get_csv_report
    """
  Запуск сбора отчёта Ozon по заданному кабинету (name) и заголовкам (headers)
  """
    try:
        logger.info(f'{name} - Выгружаю карточки товаров')
        product_info_oz = await get_product_info_attributes(api_key=api_key, client_id=client_id, name=name, sessions=sessions)
    except Exception:
        logger.exception('...')

    try:
        logger.info(f"📦 [{name}] Начинаю сбор отчета по кабинету")

        df_stocks = await get_product_list_stocks(api_key=api_key, client_id=client_id, sku=extract_sku(name=name, data_attributes=product_info_oz), name=name, sessions=sessions)

        group_df = prepare_final_ozon_data(
            df_info=read_product_info_json(
                name=name, data_attributes=product_info_oz),
            df_stocks=df_stocks,
            name=name
        )
        return group_df
    except Exception as e:
        logger.error(f"КАБИНЕТ {name} — 💥 Ошибка в execute_run_ozon: {e}")
        raise

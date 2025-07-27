from scripts.utils.setup_logger import make_logger
from scripts.utils.telegram_logger import send_tg_message
import aiohttp
import pandas as pd

logger = make_logger(__name__, use_telegram=False)


async def execute_run_ozon(api_key: str, client_id: str, name: str, sessions: aiohttp.ClientSession) -> pd.DataFrame:
    """
    🚀 Асинхронная функция для сбора и обработки отчета по товарам и остаткам из кабинета Ozon.

    Функция выполняет полный цикл:
    1. Выгружает атрибуты карточек товаров (`get_product_info_attributes`);
    2. Извлекает SKU (`extract_sku`);
    3. Получает информацию об остатках (`get_product_list_stocks`);
    4. Объединяет карточки и остатки, формирует финальный датафрейм (`prepare_final_ozon_data`).

    Используется как часть пайплайна массовой обработки кабинетов Ozon (например, через `main_run_ozon`).

    ─────────────────────────────────────────────────────────────

    🔧 Параметры:
    -------------
    api_key : str
        API-ключ от кабинета Ozon (для авторизации в запросах).

    client_id : str
        Идентификатор клиента Ozon.

    name : str
        Название кабинета, используется для логирования и Telegram уведомлений.

    sessions : aiohttp.ClientSession
        Асинхронная HTTP-сессия (общая на все запросы).

    📤 Возвращает:
    --------------
    pd.DataFrame
        Обработанный датафрейм, содержащий:
        - SKU, характеристики карточек, остатки, размеры, ссылки на фото и прочее.

    ⚠️ Обработка ошибок:
    ---------------------
    - При ошибке на любом этапе формируется лог и отправляется уведомление в Telegram.
    - Исключения пробрасываются выше для обработки в вызывающей функции.

    🧠 Автор: Илья  
    🗓 Версия: Июль 2025
    """

    from scripts.postprocessors.ozon_data_transform import prepare_final_ozon_data
    from scripts.pipelines_oz.get_cards_list_oz import extract_sku, get_product_info_attributes, read_product_info_json
    from scripts.pipelines_oz.get_stocks_oz import get_product_list_stocks

    try:
        logger.info(f'{name} - Выгружаю карточки товаров')
        send_tg_message(
            f'📦 {name} — начинаю загрузку карточек товаров (get_product_info_attributes)')

        product_info_oz = await get_product_info_attributes(api_key=api_key, client_id=client_id, name=name, sessions=sessions)

    except Exception:
        msg = f"❌ {name} — ошибка при загрузке карточек: {e}"
        logger.exception(msg)
        send_tg_message(msg)
        raise

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
        msg = f"КАБИНЕТ {name} — 💥 Ошибка в execute_run_ozon: {e}"
        logger.error(msg)
        send_tg_message(msg)
        raise

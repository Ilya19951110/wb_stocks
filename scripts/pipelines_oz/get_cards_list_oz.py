from scripts.utils.config.factory import get_requests_url_oz, get_headers
from scripts.utils.setup_logger import make_logger
from dotenv import load_dotenv
import pandas as pd
import aiohttp

load_dotenv()


logger = make_logger(__name__, use_telegram=True)


async def get_product_info_attributes(api_key: str, client_id: str, name: str, sessions: aiohttp.ClientSession) -> list[dict[str, object]]:
    """
    Асинхронно получает список товаров с атрибутами из Ozon API (product_info_attributes).

    Делает постраничные запросы к API с фильтром по видимости (ALL),
    собирает все записи до тех пор, пока не достигнет конца списка (`last_id`).

    Параметры:
    ----------
    api_key : str
        Ключ API от Ozon (API key).
    client_id : str
        ID клиента от Ozon.
    name : str
        Название аккаунта (не используется в теле функции, но может быть полезно для логирования).
    sessions : aiohttp.ClientSession
        Сессия aiohttp для выполнения асинхронных HTTP-запросов.

    Возвращает:
    -----------
    list[dict[str, object]]
        Список словарей с информацией о товарах, каждый словарь содержит данные по одному товару.

    Исключения:
    -----------
    Exception
        Генерируется, если ответ от API имеет статус, отличный от 200, с сообщением из тела ответа.

    Пример:
    -------
        >>> async with aiohttp.ClientSession() as session:
        >>>  data = await get_product_info_attributes(api_key, client_id, "MyStore", session)
        >>> print(len(data))
    """

    url = get_requests_url_oz()['product_info_attributes']
    all_rows = []

    last_id = None
    limit = 1000

    while True:
        try:
            params = {
                "filter": {

                    "visibility": "ALL"
                },
                "limit": limit,
                'last_id': last_id,
                "sort_dir": "ASC"
            }

            logger.info(
                f"{name} - Отправляю запрос к Ozon API с last_id={last_id}")

            async with sessions.post(url, headers=get_headers(api_key=api_key, client_id=client_id), json=params) as response:
                if response.status != 200:
                    text = await response.text()
                    raise Exception(f"Ошибка {response.status}: {text}")

                res = await response.json()

                items = res['result']
                logger.info(f"{name} - Получено: {len(items)} товаров")

                all_rows.extend(items)

                last_id = res.get('last_id')
                logger.info(f"{name} - Следующий last_id: {last_id}")

                if len(items) < limit:
                    logger.warning(
                        f"{name} - 🚫 Получены все данные. Завершение итерации.")
                    break

        except Exception as e:
            logger.error(f"{name} - 💥 Ошибка при получении данных: {e}")

    logger.info(
        f"{name} - ✅ Общее количество полученных карточек: {len(all_rows)}")

    return all_rows


def extract_sku(name: str, data_attributes: list[dict[str, object]]) -> list[int]:
    logger.info(
        f"{name} - Извлекаю список SKU из {len(data_attributes)} карточек")

    skus = [item['sku'] for item in data_attributes if 'sku' in item]

    logger.info(f"{name} - Извлечено {len(skus)} SKU")
    return skus


def read_product_info_json(name: str, data_attributes: list[dict[str, object]]) -> pd.DataFrame:

    all_data = []

    try:
        logger.info(
            f"{name} - Начинаю обработку {len(data_attributes)} карточек")
        for item in data_attributes:
            info = {
                'Бренд в одежде и обуви': None,
                'Объединить на одной карточке': None,
                'Цвет товара': None,
                'Тип': None,
                'Артикул': item.get('offer_id'),
                'Название товара': item.get('name'),
                'Штрихкод': item.get('barcode'),
                'Ширина упаковки, мм': item.get('width'),
                'Высота упаковки, мм': item.get('height'),
                'Длина упаковки, мм': item.get('depth'),
                'Ссылка на главное фото': item.get('primary_image'),
                'sku': item.get('sku')

            }

            for attr in item.get('attributes', []):
                attr_id = attr.get('id')
                values = attr.get('values', [])

                if not values:
                    continue

                value = values[0].get('value')

                if attr_id == 31:
                    info['Бренд в одежде и обуви'] = value
                elif attr_id == 8292:
                    info['Объединить на одной карточке'] = value
                elif attr_id in (10096, 10097):
                    info['Цвет товара'] = value
                elif attr_id in (4501, 4503):
                    info['Тип'] = value

            all_data.append(info)

        df = pd.DataFrame(all_data)
        logger.info(
            f"{name} - ✅ DataFrame сформирован: {df.shape[0]} строк, {df.shape[1]} колонок")
        return df

    except Exception as e:
        logger.error(f"{name} - 💥 Ошибка при разборе JSON: {e}")
        return pd.DataFrame()

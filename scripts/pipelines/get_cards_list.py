from scripts.utils.config.factory import get_requests_url_wb
from scripts.utils.setup_logger import make_logger
from scripts.utils.telegram_logger import send_tg_message
from datetime import datetime
import pandas as pd
import asyncio
import aiohttp

logger = make_logger(__name__, use_telegram=False)


async def get_cards(session: aiohttp.ClientSession, name: str, api: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    📦 Получение карточек товаров (ID KT, баркоды, размеры и др.) из кабинета WB.

    Асинхронная функция выполняет постраничный запрос к API Wildberries для получения
    всех карточек товаров (артикулов WB) с привязкой к внутреннему идентификатору 
    `ID KT`, баркодами и другими характеристиками.

    Используется для построения справочника товаров и соответствий.

    ───────────────────────────────────────────────────────────────

    🔧 Параметры:
    ------------
    session : aiohttp.ClientSession
        Активная асинхронная сессия для HTTP-запросов.

    name : str
        Название кабинета (используется в логировании и сообщениях Telegram).

    api : str
        Токен авторизации WB API для доступа к кабинету.

    📤 Возвращает:
    --------------
    Tuple[pd.DataFrame, pd.DataFrame]
        - Первый DataFrame `res_idkt_save`: таблица соответствий артикулов WB, ID KT, брендов, размеров, баркодов и габаритов.
        - Второй DataFrame `idkt_nmid`: минимальный справочник `Артикул WB`, `ID KT`, `updatedAt`.

    📌 Формат таблицы `res_idkt_save`:
    ---------------------------------
    | Артикул WB | ID KT | Наименование | Бренд | Размер | Баркод | Артикул поставщика | Категория | Фото | Ширина | Высота | Длина |

    🧪 Обработка:
    ------------
    - Запрос осуществляется постранично, с поддержкой курсора (updatedAt, nmID).
    - Удаляется поле `description`, чтобы уменьшить нагрузку.
    - Фото берётся первое (`photos[0]['big']`), если есть.
    - Баркоды извлекаются из `sizes -> skus`.

    🚨 Ошибки:
    ---------
    - При ошибке запроса отправляется сообщение в Telegram.
    - Некорректный формат ответа или отсутствие курсора логгируются отдельно.
    - После каждой страницы — задержка `DELAY` секунд (для избежания rate-limit).

    🧠 Автор: Илья  
    🗓 Версия: Июль 2025
    """
    DELAY = 3
    url = get_requests_url_wb()['card_list']
    all_cards, rows, cursor = [], [], None

    while True:

        headers = {"Authorization": api}

        payload = {
            "settings": {
                "sort": {"ascending": False},
                "filter": {"withPhoto": -1},
                "cursor": {"limit": 100},
                "period": {
                    'begin': '2024-01-01',
                    'end': datetime.now().strftime("%Y-%m-%d")
                },
            },
        }

        if cursor:
            payload["settings"]["cursor"].update(cursor)

        try:
            async with session.post(url, headers=headers, json=payload) as response:

                if response.status != 200:
                    error_text = await response.text()

                    msg = f"⚠️ ⚠️ Ошибка по запросу 'IDKT': {error_text}, {name}"
                    send_tg_message(msg)
                    logger.error(msg)
                    raise ValueError(msg)

                cards_list = await response.json()
        except Exception as e:
            msg = f"❌❌ Произошла ошибка при запросе к {name}: {e}"
            logger.error(msg)
            send_tg_message(msg)

        else:
            if 'cards' not in cards_list or 'cursor' not in cards_list:
                logger.error('❌ Неверный формат ответа!')
                break

            all_cards.extend([
                {k: v for k, v in card.items() if k != 'description'}
                for card in cards_list['cards'] if isinstance(card, dict)
            ])

            if not cards_list or 'cards' not in cards_list or len(cards_list['cards']) < 100:
                break

            if "updatedAt" in cards_list['cursor'] and 'nmID' in cards_list['cursor']:
                cursor = {
                    "updatedAt": cards_list['cursor']['updatedAt'],
                    "nmID": cards_list['cursor']['nmID']
                }
            else:
                msg = f"⚠️ В ответе от {name} отсутствует 'updatedAt' или 'nmID'"
                logger.error(msg)
                send_tg_message(msg)
                break

            logger.info('Спим ⏳⏳⏳')

            await asyncio.sleep(DELAY)

    if all_cards:
        for card in all_cards:
            info = {
                'Артикул WB': card['nmID'],
                'ID KT': card['imtID'],
                'Наименование': card['title'],
                'Артикул поставщика': card['vendorCode'],
                'Бренд': card['brand'],
                'Категория': card['subjectName'],
                'Фото': card['photos'][0]['big'] if card.get('photos') else None,
                'Ширина': card['dimensions']['width'],
                'Высота': card['dimensions']['height'],
                'Длина': card['dimensions']['length'],
                'updatedAt': card['updatedAt']
            }
            for size in card.get('sizes', []):
                for barcode in size.get('skus', []) or [None]:
                    row = info.copy()
                    row.update({
                        'Размер': size.get('techSize'),
                        'chrtID': size.get('chrtID'),
                        'Баркод': barcode if barcode else None
                    })
                rows.append(row)

    result = pd.DataFrame(rows)


#  Оставляем только нужные столбцы в датафрейм
    res_idkt_save = result.filter(
        ['Артикул WB', 'ID KT', 'Наименование', 'Бренд', 'Размер', 'Баркод', 'Артикул поставщика', 'Категория', 'Фото', 'Ширина', 'Высота', 'Длина'])
    idkt_nmid = result[['Артикул WB', 'ID KT', 'updatedAt']]

    logger.info(
        f"📦 Данные кабинета {name} успешно распакованы и преобразованы в DataFrame: {result.shape}\nidkt_nmid сохранен: {idkt_nmid.shape}")

    logger.info(
        f"💾 Готовлюсь сохранить {name}_cards — строк: {res_idkt_save.shape[0]}")

    return res_idkt_save, idkt_nmid

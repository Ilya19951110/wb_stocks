import pandas as pd
import asyncio
from scripts.setup_logger import make_logger
from datetime import datetime


async def get_cards(session, name, api):
    logger = make_logger(__name__, use_telegram=True)
    DELAY = 3
    url_cards = "https://content-api.wildberries.ru/content/v2/get/cards/list"
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
            async with session.post(url_cards, headers=headers, json=payload) as response:

                if response.status != 200:
                    error_text = await response.text()
                    logger.error(
                        f"⚠️ ⚠️ Ошибка по запросу 'IDKT': {error_text}, {name}")
                    raise ValueError(error_text)

                cards_list = await response.json()
        except Exception as e:
            logger.error(f"❌❌ Произошла ошибка при запросе к {name}: {e}")

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
                logger.error(
                    f"⚠️ В ответе от {name} отсутствует 'updatedAt' или 'nmID'")
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
    return res_idkt_save, idkt_nmid

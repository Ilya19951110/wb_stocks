from gspread.exceptions import WorksheetNotFound, APIError
from collections import defaultdict
import requests
import json
from datetime import datetime, timedelta
import pandas as pd
import os
import time
import gspread
from gspread_dataframe import set_with_dataframe

# Метод предоставляет количество остатков товаров на складах WB. Данные обновляются раз в 30 минут. Нужен!!!


def query_stocks(cabinet, hdrs):

    url = "https://statistics-api.wildberries.ru/api/v1/supplier/stocks"

    headers = {"Authorization": hdrs}
    # тело запроса
    params = {
        "dateFrom": "2024-01-01"  # datetime.now().strftime('%Y-%m-%d')
    }
    # запрос
    res = requests.get(url, headers=headers, params=params)
    print(f'подключение к api/v1/supplier/stocks:\n{res}')
    result = res.json()
    data_stoks = pd.DataFrame(result)
    # 1. Создаем основную таблицу с остатками

    data_stoks = data_stoks.rename(columns={
        'nmId': 'Артикул WB',
        'lastChangeDate': 'Справка',
        'brand': 'Бренд',
        'techSize': 'Размер',
        'quantityFull': 'Итого остатки',
        'barcode': 'Баркод',
        'Price': 'Цена',
        'Discount': 'Скидка',
        'supplierArticle': 'Артикул поставщика'})
    # преобразуем столбец спарвка в нужный формат даты, например 2025-01-01
    data_stoks['Справка'] = pd.to_datetime(
        data_stoks['Справка'], format='ISO8601').dt.date

    # сортируем в порядке убывания
    df_sort = data_stoks.sort_values('Справка', ascending=False)

    # создаем новый столбец и подставяем туда последнюю актуальную цену
    lasted_max_price = df_sort.loc[df_sort.groupby('Артикул WB',)[
        'Цена'].idxmax()]

    # создаем новый столбец и подставяем туда последнюю актуальную цену скидку
    lasted_min_discount = df_sort.loc[df_sort.groupby('Артикул WB')[
        'Скидка'].idxmax()]

    df_sort = df_sort.merge(
        lasted_max_price[[
            'Артикул WB', 'Цена'
        ]].rename(columns={'Цена': 'Макс_цена'}),
        on='Артикул WB',
        how='left'

    )

    df_sort = df_sort.merge(
        lasted_min_discount[[
            'Артикул WB', 'Скидка'
        ]].rename(columns={'Скидка': 'Макс_скидка'}),
        on='Артикул WB',
        how='left'
    )

    df_sort['Цена'] = df_sort['Макс_цена']
    df_sort['Скидка'] = df_sort['Макс_скидка']

    df_sort = df_sort.drop(['Макс_цена', 'Макс_скидка'], axis=1)

    print(f'Остатки {cabinet} сохранены')

    return df_sort


# ----------------------------------------------------------------------------------------------------------------


# Метод предоставляет список созданных карточек товаров.
# с последующией распаковкой в дата фрейм
def query_in_idkt(cabinet, hdrs):
    DELAY = 0.7  # задержка при выполнении кода
    #
    url_cards = "https://content-api.wildberries.ru/content/v2/get/cards/list"

    # итоговый список карточек
    all_cards = []
    rows = []
    cursor = None

    while True:
        headers = {"Authorization": hdrs}
        # Формируем тело запроса
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

        # Добавляем cursor для пагинации
        if cursor:
            payload["settings"]["cursor"].update(cursor)

        # Отправляем запрос
        response = requests.post(url_cards, headers=headers, json=payload)
        print(
            f'подключение к content/v2/get/cards/list:\n {response}, {cabinet}')
        # проверяем статус запроса 200 -  это успешное подключение
        if response.status_code != 200:
            print(f"Ошибка: {response.status_code}")
            print(response.text)
            break

        data = response.json()

        # Проверяем структуру ответа
        if 'cards' not in data or 'cursor' not in data:
            print("Некорректный формат ответа:")
            print(json.dumps(data, indent=4, ensure_ascii=False))
            break

        # перебераем данные в словре data'cards'
        for card in data['cards']:
            # проверяет, является ли текущий элемент card словарем
            if isinstance(card, dict):
                # создаем новый словарь
                all_cards.append({
                    # берем все пары key:val из card
                    key: val for key, val in card.items()
                    # исключаем key description - это описание
                    if key not in ['description']
                })

        if not data or 'cards' not in data or len(data['cards']) < 100:
            # if len(data['cards']) < 100:
            break

        # Обновляем курсор
        cursor = {
            "updatedAt": data['cursor']['updatedAt'],
            "nmID": data['cursor']['nmID']
        }

        time.sleep(DELAY)

    print(f"длина all_cards {len(all_cards)}")
    print(f'Список карточек {cabinet} выгружен')

    if all_cards:
        for card in all_cards:
            info = {
                'Артикул WB': card['nmID'],
                'ID КТ': card['imtID'],
                'Наименование': card['title'],
                'Артикул поставщика': card['vendorCode'],
                'Бренд': card['brand'],
                'Категория': card['subjectName'],
                'Фото': card['photos'][0]['big'] if card.get('photos') else None,
                'Ширина': card['dimensions']['width'],
                'Высота': card['dimensions']['height'],
                'Длина': card['dimensions']['length']
            }
            # Распаковываем соварь sizes, нам нужен размер и баркод
            for size in card.get('sizes', []):
                for barcode in size.get('skus', []) or [None]:
                    row = info.copy()
                    row.update({
                        'Размер': size.get('techSize'),
                        'chrtID': size.get('chrtID'),
                        'Баркод': barcode if barcode else None
                    })
                rows.append(row)
    # преобразуем в датафрейм
    result = pd.DataFrame(rows)
    #  Оставляем только нужные столбцы в датафрейм
    res_idkt_save = result.filter(
        ['Артикул WB', 'ID КТ', 'Наименование', 'Бренд', 'Размер', 'Баркод', 'Артикул поставщика', 'Категория', 'Фото', 'Ширина', 'Высота', 'Длина'])
    print(res_idkt_save.columns.tolist())
    print(f'карточки товаров {cabinet} распакованы')
    return res_idkt_save


# ---------------------


# объединение остатков  с id кт
def combain_query(stocks, IDKT, cabinet):

    stocks['Артикул WB'] = stocks['Артикул WB'].astype(int)
    stocks['Баркод'] = stocks['Баркод'].astype(str)
    stocks['Размер'] = stocks['Размер'].astype(str)

    IDKT['Артикул WB'] = IDKT['Артикул WB'].astype(int)
    IDKT['Баркод'] = IDKT['Баркод'].astype(str)
    IDKT['Размер'] = IDKT['Размер'].astype(str)
    IDKT['ID КТ'] = IDKT['ID КТ'].astype(int)

    # Объединяем две таблицы остатки цепляем к idkt
    result = pd.merge(
        IDKT,
        stocks,
        on=['Артикул WB', 'Баркод'],
        how='outer',
        indicator=True,
        suffixes=('_IDKT', '_stocks')
    )

    # Удаляем не нужные столбцы
    result = result.drop(columns=[col for col in result.columns if col.endswith('_stocks')]+['Справка', 'warehouseName',
                         'quantity', 'inWayToClient', 'inWayFromClient', 'category', 'subject', 'isRealization', 'SCCode', 'isSupply'])

    # удаляем суффиксы _IDKT у столбцов, которые остались
    result.columns = [
        col.replace('_IDKT', '') for col in result.columns
    ]

    # выбираем столбцы
    num_col = ['Цена', 'Скидка',
               'Итого остатки', 'Ширина', 'Высота', 'Длина']
    string_cols = ['Бренд', 'Размер', 'Категория', 'Наименование',
                   ]

    # Заполняем NAN в Цена и Скидка последними известными знач для артикула
    result[['Цена', 'Скидка']] = result.groupby(
        'Артикул WB')[['Цена', 'Скидка']].ffill()
    # заполняем пустоты нужными значениями
    result[num_col] = result[num_col].fillna(0)
    result[string_cols] = result[string_cols].fillna('-')

    # сохраняем только те строки, которые есть в таблице stocks остатки
    right_only_rows = result[result['_merge'] == 'right_only']

    # в осноном дф удаляем строки которые есть только в правой таблице, они косячные
    result = result[result['_merge'] != 'right_only']

    # удаляем столбец _merge
    result = result.drop(columns='_merge')

    # группируем по столбцу итого остатки
    result = result.groupby([
        col for col in result.columns if col != 'Итого остатки'
    ])['Итого остатки'].sum().reset_index()

    # Создаем новый столбец Цена до СПП
    result['Цена до СПП'] = result['Цена'] * \
        (1 - result['Скидка']/100)

    # дф с артикулом и баркодом
    barcode_nmid = result.filter([
        'Артикул WB', 'Баркод', 'Размер'
    ])

    result = result.drop(columns=[
        'Баркод', 'Размер'
    ])

    result[['Цена', 'Скидка', 'Цена до СПП']] = result.groupby(
        'Артикул WB')[['Цена', 'Скидка', 'Цена до СПП']].transform('first')

    # группировка после удаления по сумме остатков
    result = result.groupby([
        col for col in result.columns if col != 'Итого остатки'
    ])['Итого остатки'].sum().reset_index()

    new_order = [
        'Артикул WB', 'ID КТ', 'Артикул поставщика', 'Бренд', 'Наименование', 'Категория',
        'Итого остатки', 'Цена', 'Скидка', 'Цена до СПП', 'Фото', 'Ширина', 'Высота', 'Длина'
    ]

    # применяем новое расположение
    result = result[new_order]
    # фильтруем по убыванию
    result['Фото'] = result['Фото'].apply(
        lambda url: f'=IMAGE("{url}", 4, 20, 81)' if pd.notna(url) else '')

    result = result.sort_values('Итого остатки', ascending=False)
    result['Кабинет'] = cabinet

    if len(right_only_rows) > 0:
        print(f"косячная карточка кабинета {cabinet} =  {right_only_rows.shape}",
              right_only_rows['Артикул WB'], sep='\n')

    else:
        print(f'косячных карточек {cabinet} нет', '', sep='\n')

    print(
        f"Есть ли дубликаты в {cabinet}?", result.duplicated().any(),
        f"артикул и баркод {cabinet}: {barcode_nmid.shape}",
        sep='\n'
    )

    return result, barcode_nmid


def save_in_gsh(dick_data):

    all_cabinet = pd.DataFrame()
    # сервис аккаунт гугл
    gc = gspread.service_account(
        filename='key.json')
    # открываем гугл таблицу
    spreadsheet = gc.open('Ассортиментная матрица. Полная')

    # Создание рабочего листа

    try:
        worksheet_block = None

        try:
            print('получаем доступ к worksheet_block')
            worksheet_block = spreadsheet.worksheet('БЛОК')
        except WorksheetNotFound as e:
            print(f"[ОШИБКА] Лист БЛОК не найден: {e}")

        except APIError as e:
            print(f"[ОШИБКА] Проблема с доступом к листу БЛОК: {e}")

        except Exception as e:
            print(f"[НЕПРЕДВИДЕННАЯ ОШИБКА] при работе с листом БЛОК: {e}")

        if worksheet_block:
            try:
                print("Получаем список заблокированных nmid из листа 'БЛОК'")
                block_nmid = set([
                    int(row[1])

                    for row in worksheet_block.get_all_values()[1:]
                    if row[0].strip().isdigit() and int(row[0]) == 0
                ])
                print(f"Получено {len(block_nmid)} заблокированных nmid")

            except Exception as e:
                print(
                    f"[ОШИБКА] ❌ Не удалось прочитать данные из листа 'БЛОК': {e}")
                block_nmid = set()

        else:
            print("⚠️ Ошибка: лист 'БЛОК' не доступен — данные не загружены")
            block_nmid = set()

    except Exception as e:
        print(f"\033[91m[ОШИБКА]\033[0m при получении block_nmid: {e}")
        block_nmid = set()

    try:
        worksheet_idkt = None
        try:

            print("Получаем доступ к листу 'API'")
            worksheet_idkt = spreadsheet.worksheet('API')
        except WorksheetNotFound as e:
            print(f"[ОШИБКА] Лист 'API не найден: {e}")

        except APIError as e:
            print(f"[ОШИБКА] Проблема с доступом к листу API: {e}")

        except Exception as e:
            print(f"[НЕПРЕДВИДЕННАЯ ОШИБКА] при работе с листом API: {e}")

        if worksheet_idkt:
            try:
                print('Объединяем все кабинеты (df[0]) и фильтруем по условию')

                all_cabinet = pd.concat([df_tuple[0]
                                        for df_tuple in dick_data.values()], ignore_index=True)

                all_cabinet = all_cabinet[~all_cabinet['Артикул WB'].isin(
                    block_nmid)]

                if all_cabinet.empty:
                    print("⚠️ DataFrame all_cabinet пуст — пропущена выгрузка.")
                else:
                    worksheet_idkt.clear()

                    worksheet_idkt.update(
                        [all_cabinet.columns.values.tolist()] + all_cabinet.values.tolist())
                    print('Данные all_cabinet выгружены в лист API')
            except Exception as e:
                print(
                    f"[ОШИБКА] ❌ Ошибка при формировании или выгрузке all_cabinet в лист API: {e}")
        else:
            print("Пропущена выгрузка all_cabinet: лист 'API' не доступен")

    except Exception as e:
        print(
            f"\033[91m[ОШИБКА]\033[0m Общая ошибка при обработке all_cabinet: {e}")

    try:
        try:
            worksheet_barcode = spreadsheet.worksheet('API 2')
        except WorksheetNotFound as e:
            print(f"[ОШИБКА] Лист 'API 2' не найден: {e}")
        except APIError as e:
            print(f"[ОШИБКА] Проблема с доступом к листу 'API 2': {e}")
            worksheet_idkt = None
        except Exception as e:
            print(f"[НЕПРЕДВИДЕННАЯ ОШИБКА] при работе с листом 'API 2': {e}")
            worksheet_idkt = None

        if worksheet_barcode:
            print('Объединяем все баркода (df[1]) из dick_data"')
            # выгружаем и объединяем все баркода
            barcode = pd.concat([
                df_tuple[1] for df_tuple in dick_data.values()
            ], ignore_index=True)
            print(f"Всего баркодов {len(barcode)}")

            worksheet_barcode.clear()
            worksheet_barcode.update([
                barcode.columns.values.tolist()
            ] + barcode.values.tolist()
            )
            print(f"📤 Загружено в лист 'API 2': {barcode.shape[0]} строк")

        else:
            print("Пропущена выгрузка barcode: лист 'API 2' не доступен")

    except Exception as e:
        print(
            f"\033[91m[ОШИБКА]\033[0m при формировании или выгрузке barcode: {e}")

    b_data = defaultdict(pd.DataFrame)
    for name, (_, bcode_df) in dick_data.items():

        if name in ['Азарья', 'Рахель', 'Михаил']:
            b_data['Фин модель Иосифовы Р А М'] = pd.concat([
                b_data['Фин модель Иосифовы Р А М'], bcode_df
            ], ignore_index=True)

        if name in ['Галилова']:
            b_data['Фин модель Галилова'] = pd.concat([
                b_data['Фин модель Галилова'], bcode_df
            ], ignore_index=True)

        if name in ['Мартыненко', 'Торгмаксимум']:
            b_data['Фин модель Мартыненко и Торгмаксимум'] = pd.concat(
                [b_data['Фин модель Мартыненко и Торгмаксимум'], bcode_df], ignore_index=True)

        # загрузка баркодов по кабинетам
    for sheets, df in b_data.items():
        try:
            print(f"Открываем таблицу {sheets}")
            sh = gc.open(sheets)
            wks = None
            try:
                wks = sh.worksheet('API WB barcode')
            except WorksheetNotFound as e:
                print(
                    f"[ОШИБКА] Лист 'API WB barcode' не найден в таблице {sheets}: {e}")

            except APIError as e:
                print(
                    f"[ОШИБКА] Проблема с доступом к листу 'API WB barcode' в таблице {sheets}: {e}")

            except Exception as e:
                print(
                    f"[НЕПРЕДВИДЕННАЯ ОШИБКА] при работе с листом 'API WB barcode' в таблице {sheets}: {e}")

            if wks:
                try:
                    if df.empty:
                        print(
                            f"⚠️ Внимание: DataFrame для '{sheets}' пуст — пропускаем выгрузку.")
                        continue

                    wks.clear()
                    wks.update([df.columns.values.tolist()] +
                               df.values.tolist())

                    print(
                        f'Баркод загружен в таблицу {sheets}\nДлина: {df.shape}')

                except Exception as e:
                    print(
                        f"[ОШИБКА] ❌ Ошибка при выгрузке данных в '{sheets}': {e}")
            else:
                print(
                    f"Данные не выгружены в таблицу {sheets}: лист недотупен")

        except Exception as e:
            print(
                f"\033[91m[ОШИБКА]\033[0m в таблице '{sheets}': {e}\n данные не выгружены")


if __name__ == '__main__':

    begin = time.time()
    all_cabinet = {}

    all_api_io = {
        'Азарья': os.getenv('Azarya').strip(),
        'Михаил': os.getenv('Michael').strip(),
        'Рахель': os.getenv('Rachel').strip(),
        'Галилова': os.getenv('Galilova').strip(),
        'Торгмаксимум': os.getenv('TORGMAKSIMUM').strip(),
        'Мартыненко': os.getenv('Martynenko').strip(),
        'Сергей': os.getenv('Sergey').strip(),
    }

    for k, v in all_api_io.items():
        all_cabinet[k] = combain_query(
            query_stocks(cabinet=k, hdrs=v),
            query_in_idkt(cabinet=k, hdrs=v),
            cabinet=k
        )

    for name, df in all_cabinet.items():
        print(f"{name}: {len(df[1])}, тип {type(df)}")

    save_in_gsh(all_cabinet)
    end = time.time()
    print(f"Время выполнения: {end-begin}")

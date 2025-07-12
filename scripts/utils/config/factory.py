import os


def get_all_api_ozon() -> dict[str, dict[str, str]]:
    """
    Возвращает данные по кабинетам ozon Havva, Gabriel, UCARE

    """
    return {
        'Havva': {

            'Client-Id': os.getenv('HAVVA_Client_id_oz'),
            'Api-Key': os.getenv('HAVVA_api_key_oz'),
            'Content-Type': 'application/json'
        },
        'Gabriel': {
            'Client-Id': os.getenv('Gabriel_Client_id_oz'),
            'Api-Key': os.getenv('Gabriel_api_key_oz'),
            'Content-Type': 'application/json',
        },
        'UCARE': {
            'Client-Id': os.getenv('UCARE_Client_id_oz'),
            'Api-Key': os.getenv('UCARE_api_key_oz'),
            'Content-Type': 'application/json',
        }
    }


def get_all_api_request() -> dict[str, str]:
    """
   Возвращает маппинг: кабинет → API-ключ из .env
   """
    return {
        'Азарья': os.getenv('Azarya').strip(),
        'Михаил': os.getenv('Michael').strip(),
        'Рахель': os.getenv('Rachel').strip(),
        'Галилова': os.getenv('Galilova').strip(),
        'Мартыненко': os.getenv('Martynenko').strip(),
        'Мелихов': os.getenv('Melikhov').strip()
    }


def get_group_map() -> dict[str, list[str]]:
    """
Возвращает маппинг: имя таблицы Google Sheets → список кабинетов, например:
'Фин модель Иосифовы Р А М': ['Азарья', 'Рахель', 'Михаил'],
"""
    return {
        'Фин модель Иосифовы Р А М': ['Азарья', 'Рахель', 'Михаил'],
        'Фин модель Галилова': ['Галилова'],
        'Фин модель Мартыненко': ['Мартыненко', 'Торгмаксимум']
    }


def tables_names() -> dict[str, str]:
    """
    Возвращае словарь key, val - имя таблицы
    'profit_supplier': 'Прибыль поставщики',
    'wb_matrix_complete': 'Ассортиментная матрица. Полная',
    'oz_matrix_complete': 'Ассортиментная матрица OZON'
    """
    return {
        'profit_supplier': 'Прибыль поставщики',
        'wb_matrix_complete': 'Ассортиментная матрица. Полная',
        'oz_matrix_complete': 'Ассортиментная матрица OZON'
    }


def get_assortment_matrix_complete() -> str:
    return 'Ассортиментная матрица. Полная'


def get_assortment_matrix_complete_OZON() -> str:
    return 'Ассортиментная матрица OZON'


def get_requests_url_oz() -> dict[str, str]:
    """
    'product_create' - Метод для получения отчёта с данными о товарах
    'report_info' - Возвращает информацию о созданном ранее отчёте по его идентификатору.
    'analytics_stocks'  - Метод для того чтобы получить аналитику по остаткам товаров на складах.
    'analytics_data' -  это доступ к маркетинговой и торговой аналитике по товару (или группе товаров) в личном кабинете Ozon.
    'advert_campaing_oz' - Возвращает список рекламных кампаний
    'statistics_advert_campaing_oz' - возвращает статистику по рекламным кампниям озона
    'product_info_attributes' - Возвращает описание характеристик товаров по идентификатору и видимости
    """
    return {
        'product_create': 'https://api-seller.ozon.ru/v1/report/products/create',
        'report_info': 'https://api-seller.ozon.ru/v1/report/info',
        'analytics_stocks': 'https://api-seller.ozon.ru/v1/analytics/stocks',
        'analytics_data': 'https://api-seller.ozon.ru/v1/analytics/data',
        'list_advert_campaing_oz': 'https://api-performance.ozon.ru:443/api/client/campaign',
        'statistics_advert_campaing_oz': 'https://api-performance.ozon.ru:443/api/client/statistics',
        'product_info_attributes': 'https://api-seller.ozon.ru/v4/product/info/attributes'
    }


def get_headers(api_key: str, client_id: str) -> dict[str, str]:
    return {

        'Client-Id': client_id,
        'Api-Key': api_key,
        'Content-Type': 'application/json'
    }


def get_requests_url_wb() -> dict[str, str]:
    """
    Возвращает словарь с ключом и именем url для подключения к апи wb
    """

    return {
        'promotion_count': 'https://advert-api.wildberries.ru/adv/v1/promotion/count',
        'advert_fullstats': "https://advert-api.wildberries.ru/adv/v2/fullstats",
        'card_list': "https://content-api.wildberries.ru/content/v2/get/cards/list",
        'report_detail': 'https://seller-analytics-api.wildberries.ru/api/v2/nm-report/detail',
        'supplier_stocks': "https://statistics-api.wildberries.ru/api/v1/supplier/stocks",
        'tariffs_box': "https://common-api.wildberries.ru/api/v1/tariffs/box",
    }


def get_finmodel_to_cabinet_map() -> dict[str, tuple[str, list[str]]]:
    """

    Возвращает соответствие финмоделей их кабинетам и владельцам (для объединения данных из WB и OZ)
    """

    return {
        'Фин модель Иосифовы Р А М': ('Gabriel', ['Рахель', 'Михаил', 'Азарья']),
        'Фин модель Галилова': ('Havva', ['Галилова']),
        'Фин модель Мартыненко': ('Ucare', ['Мартыненко']),
    }


def sheets_names() -> dict[str, str]:
    """
     Возвращает словарь с названиями листов Google Sheets для входных и выходных данных
     """

    return {
        'directory_wb': 'Справочник WB',
        'directory_oz': 'Справочник OZ',
        'barcodes_oz': 'Баркода OZ',
        'barcodes_wb_oz': 'Баркод WB_OZ',
        'api_mywarehouse': 'API Мой склад',
        'api_wb_barcode': 'API WB barcode',
        'api_wb_advert': 'API WB РК',
        'api_wb_sales': 'API WB Воронка',
        'group_stocks_and_idkt': 'API',
        'group_all_barcodes': 'API 2',
        'block_nmid': 'БЛОК',
        'ozon_stocks': 'API OZ Остатки'


    }

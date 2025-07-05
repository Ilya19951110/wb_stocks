import os


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


def get_assortment_matrix_complete() -> str:
    return 'Ассортиментная матрица. Полная'


def get_assortment_matrix_complete_OZON() -> str:
    return 'Ассортиментная матрица OZON'

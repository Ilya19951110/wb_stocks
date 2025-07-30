from scripts.utils.setup_logger import make_logger
from scripts.utils.gspread_client import get_gspread_client
from scripts.utils.config.factory import tables_names
from scripts.utils.telegram_logger import send_tg_message

import pandas as pd

logger = make_logger(__name__, use_telegram=False)


def push_concat_all_cabinet_stocks_to_sheets(
    data: list[pd.DataFrame],
    sheet_name: str,
    clear_range=None,
    block_nmid=None
) -> None:
    """
    📤 Выгрузка объединённых остатков по кабинетам в Google Sheets

    Функция принимает список датафреймов с остатками по кабинетам WB,
    объединяет их в одну таблицу и выгружает в указанный лист Google Таблицы.
    Поддерживается опциональная фильтрация по артикулу (блокировка NM ID)
    и очистка диапазона или всего листа перед вставкой.

    ────────────────────────────────────────────────────────────────────────────

    📥 Аргументы:
    - data (list[pd.DataFrame]):
        Список датафреймов с остатками по кабинетам (например, по партнёрам).

    - sheet_name (str):
        Название листа в Google Таблице, в который будут загружены данные.

    - clear_range (list[str] | None):
        Опционально. Список диапазонов для частичной очистки (например, ["A1:F100"]).
        Если не задано — будет очищен весь лист.

    - block_nmid (list[str] | None):
        Опционально. Список артикулов (NM ID), которые нужно исключить перед загрузкой.
        Применяется фильтрация по столбцу "Артикул WB".

    ────────────────────────────────────────────────────────────────────────────

    🔁 Алгоритм работы:

    1. Авторизация в Google Sheets.
    2. Получение таблицы `Ассортиментная матрица. Полная` и листа `sheet_name`.
    3. Объединение всех датафреймов в один.
    4. Фильтрация по блок-листу, если указан.
    5. Очистка листа (весь или по диапазону).
    6. Загрузка объединённых данных с заголовками.
    7. Логирование результатов и отправка ошибок в Telegram.

    ────────────────────────────────────────────────────────────────────────────

    🧾 Используемые утилиты:
    - `get_gspread_client()` → авторизация в Google Sheets
    - `get_assortment_matrix_complete()` → имя таблицы
    - `send_tg_message()` → уведомление об ошибках
    - `make_logger()` → логирование в файл/терминал

    ────────────────────────────────────────────────────────────────────────────

    💡 Особенности:
    - Если данные пусты, функция всё равно выгрузит только заголовки.
    - Для выгрузки используется метод `.update()` (быстрее, чем `set_with_dataframe`).
    - Исключение артикулов (`block_nmid`) применяется только если параметр передан.

    🧑‍💻 Автор: Илья  
    📅 Версия: Июль 2025
    """
    try:
        sh = tables_names()['wb_matrix_complete']
        logger.info('🔌 Подключаюсь к Google Sheets клиенту...')

        gs = get_gspread_client()
        logger.info('✅ Успешно подключен к Google Sheets!')

    except Exception:
        msg = "❌ Ошибка при подключении к Google Sheets"
        logger.exception(msg)
        send_tg_message(msg)
        return

    try:
        spreadsheet = gs.open(sh)
        worksheet = spreadsheet.worksheet(sheet_name)

        if block_nmid:
            logger.info(f'block_nmid длина {len(block_nmid)}')
            df_combined = pd.concat(data, ignore_index=True)

            df_combined = df_combined[~df_combined['Артикул WB'].isin(
                block_nmid)]

            logger.info(f"📊 Объединено строк: {len(df_combined)}")

        else:
            df_combined = pd.concat(data, ignore_index=True)
            logger.info(f"📊 Объединено строк: {len(df_combined)}")

        if clear_range:
            worksheet.batch_clear(clear_range)
            logger.info(
                f"🧼 Очищен диапазон {clear_range} в листе '{sheet_name}'")

        else:
            worksheet.clear()
            logger.info(f"🧼 Полностью очищен лист '{sheet_name}'")

        worksheet.update(

            values=[df_combined.columns.values.tolist()] +
            df_combined.values.tolist()
        )

        logger.info(f"✅ Данные успешно выгружены в лист '{sheet_name}'")
    except Exception:
        msg = f"❌ Ошибка при загрузке данных в лист '{sheet_name}'"
        logger.exception(msg)
        send_tg_message(msg)

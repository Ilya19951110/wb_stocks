from scripts.utils.setup_logger import make_logger
from scripts.utils.gspread_client import get_gspread_client
from scripts.utils.config.factory import tables_names
from scripts.utils.telegram_logger import send_tg_message
from scripts.utils.prepare_values_df import prepare_values_for_sheets
from gspread.utils import rowcol_to_a1
from typing import Optional
import pandas as pd
import time

logger = make_logger(__name__, use_telegram=False)


def filtered_blocked_nmid(df: pd.DataFrame, block_df: pd.DataFrame) -> pd.DataFrame:

    df = df.copy()
    block_df = block_df.copy()

    # Приведение колонок к строкам и очистка
    df['Артикул WB'] = df['Артикул WB'].astype(str).str.strip()
    block_df['Артикул WB'] = block_df['Артикул WB'].astype(str).str.strip()

    # Приведение остатков к числу
    df['Итого остатки'] = pd.to_numeric(
        df['Итого остатки'], errors='coerce').fillna(0)

    # Формируем маски
    block_nmid_set = set(block_df['Артикул WB'])
    in_block = df['Артикул WB'].isin(block_nmid_set)
    no_stocks = df['Итого остатки'] <= 0
    to_remove = in_block & no_stocks

    # Логирование
    logger.info(
        f"🧹 Фильтрация блокировки: удалено строк — {to_remove.sum()} из {len(df)}")

    # Отладка — показать примеры
    if to_remove.sum() > 0:
        logger.debug(
            f"🧾 Пример удалённых:\n{df[to_remove][['Артикул WB', 'Итого остатки']].head(5)}")

    # Возвращаем очищенный DataFrame
    df = df[~to_remove].copy()
    df['Артикул WB'] = df['Артикул WB'].astype(int)
    return df


def push_concat_all_cabinet_stocks_to_sheets(
    data: list[pd.DataFrame],
    sheet_name: str,
    block_nmid: Optional[pd.DataFrame] = None,
    start_range=None,

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

        for attempt in range(3):
            try:
                spreadsheet = gs.open(sh)
                break
            except Exception as e:
                logger.warning(f"📛 Попытка {attempt + 1} — ошибка: {e}")

                if attempt < 2:
                    time.sleep(3)
                else:
                    raise

        worksheet = spreadsheet.worksheet(sheet_name)

        if block_nmid is not None and not block_nmid.empty:

            logger.info(f'block_nmid длина {len(block_nmid)}')
            _df_combined = pd.concat(data, ignore_index=True)

            logger.info(f"📊 Объединено строк: {len(_df_combined)}")

            df_combined = filtered_blocked_nmid(
                df=_df_combined, block_df=block_nmid)

            logger.warning(f"После фильтрации дф: {df_combined.shape}")

        else:
            df_combined = pd.concat(data, ignore_index=True)
            logger.info(f"📊 Объединено строк: {len(df_combined)}")

        if start_range:
            cols = df_combined.shape[1]
            
            clear_range = f"{start_range}:{rowcol_to_a1(1, cols).rstrip('0123456789')}"
            worksheet.batch_clear(clear_range)
            logger.info(
                f"🧼 Очищен диапазон {clear_range} в листе '{sheet_name}'")
            values =prepare_values_for_sheets(df_combined)
        else:
            worksheet.batch_clear()
            logger.info(f"🧼 Полностью очищен лист '{sheet_name}'")
            values = prepare_values_for_sheets(df_combined) + df_combined.values.tolist(),
           
            start_range='A1'

        worksheet.update(
            values=values,
            range_name=start_range,
            value_input_option="USER_ENTERED",
        )

        logger.info(f"✅ Данные успешно выгружены в лист '{sheet_name}'")
    except Exception:
        msg = f"❌ Ошибка при загрузке данных в лист '{sheet_name}'"
        logger.exception(msg)
        send_tg_message(msg)

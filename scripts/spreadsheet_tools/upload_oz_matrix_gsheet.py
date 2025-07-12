from scripts.utils.config.factory import get_assortment_matrix_complete_OZON, sheets_names
from scripts.utils.gspread_client import get_gspread_client
from scripts.utils.setup_logger import make_logger
from scripts.utils.telegram_logger import send_tg_message
import pandas as pd
from gspread_dataframe import set_with_dataframe


logger = make_logger(__name__, use_telegram=False)


def upload_oz_stocks_oz_matrix(data: dict[str, pd.DataFrame], clear_range=['A:M']) -> None:
    """
    Загружает данные остатков Ozon в указанные листы Google Таблицы.

    Функция принимает словарь, где ключи — идентификаторы (например, склады или регионы),
    а значения — соответствующие DataFrame с остатками. Для каждого ключа создаётся или обновляется
    отдельный лист в таблице, при необходимости лист очищается перед загрузкой.

    Порядок действий:
        1. Инициализация клиента Google Sheets.
        2. Получение имени основной таблицы (матрицы ассортимента Ozon).
        3. Для каждого элемента в словаре:
            - Формируется имя листа: sheets_names()['ozon_stocks'] + ключ.
            - Проверяется наличие листа. Если он есть — очищается заданный диапазон, иначе создаётся.
            - Загружается DataFrame в лист с заголовками, без индекса.

    Args:
        data (dict[str, pd.DataFrame]): 
            Словарь с данными для загрузки. Ключ — имя листа (например, "Москва"),
            значение — pandas DataFrame с данными остатков по складу.

        clear_range (list[str], optional): 
            Диапазоны ячеек для очистки перед загрузкой. 
            По умолчанию очищаются столбцы A:M.

    Returns:
        None

    Exceptions:
        Все ошибки логируются и отправляются в Telegram.
        При ошибке выполнение конкретного шага пропускается, но цикл продолжается.
    """

    try:
        logger.info(
            '📡 Инициализирую GSpread клиента и получаю название таблицы...')

        gs = get_gspread_client()
        table = get_assortment_matrix_complete_OZON()

        logger.info(f"✅ Таблица найдена: '{table}'")

    except Exception as e:
        send_tg_message(f"❌ Ошибка при инициализации Google Sheets: {e}")
        logger.exception(
            f"Ошибка при подключении к GSpread или получении названия таблицы\n{e}")
        return

    for name, df in data.items():

        try:

            logger.info(f"📄 {name} → Начинаю обновление листа")

            sheet_name = f"{sheets_names()['ozon_stocks']}_{name}"
            spreadsheet = gs.open(table)

            work_sheets = [ws.title for ws in spreadsheet.worksheets()]

            logger.info(f"📌 {name} → Название листа: '{sheet_name}'")
        except Exception as e:
            msg = f"❌ {name} → Ошибка при открытии таблицы или листов: {e}"
            send_tg_message(msg)
            logger.exception(msg)
            return

        try:
            logger.info(f"🧼 {name} → Лист найден, очищаю и настраиваю размеры")
            if sheet_name in work_sheets:

                wsheet = spreadsheet.worksheet(sheet_name)

                wsheet.batch_clear(clear_range)

            else:

                logger.info(f"🆕 {name} → Лист не найден, создаю новый")
                wsheet = spreadsheet.add_worksheet(
                    title=sheet_name, rows=df.shape[0], cols=df.shape[1])

        except Exception as e:
            msg = f"❌ {name} → Ошибка при работе с листом: {e}"
            send_tg_message(msg)
            logger.exception(msg)
            return

        try:
            logger.info(f"📤 {name} → Загружаю DataFrame на лист")

            set_with_dataframe(
                wsheet,
                df,
                col=1,
                row=1,
                include_column_header=True,
                include_index=False
            )

            logger.info(
                f"✅ {name} → Успешно загружено: {df.shape[0]} строк, {df.shape[1]} столбцов")
        except Exception as e:
            msg = f"❌ {name} → Ошибка при загрузке данных: {e}"
            send_tg_message(msg)
            logger.exception(msg)

    send_tg_message(
        "✅ Загрузка всех данных в Google Sheets завершена успешно!")
    logger.info("🏁 Все данные загружены")

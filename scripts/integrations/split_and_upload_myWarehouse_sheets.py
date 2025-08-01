import pandas as pd
from scripts.utils.gspread_client import get_gspread_client
from scripts.utils.setup_logger import make_logger
from scripts.utils.config.factory import sheets_names, tables_names
from gspread_dataframe import set_with_dataframe
from scripts.utils.telegram_logger import send_tg_message

logger = make_logger(__name__, use_telegram=False)


def upload_mywerehouse_in_gsheets() -> None:
    """
    Загружает данные из листа MyWarehouse в одну Google Таблицу в другой.

    Последовательно выполняет:
    1. Инициализацию клиента Google Sheets (через gspread).
    2. Чтение данных из листа `API Мой склад` в таблице `Асртиментная матрица полная`.
    3. Подготовку таблицы `Прибыль поставщики`: очистка существующего листа или создание нового.
    4. Загрузку считанных данных в соответствующий лист: 'API Мой склад'.

    Все действия логируются, в случае ошибок отправляются уведомления в Telegram и пишутся исключения в лог.

    Исключения:
        При любой ошибке происходит логгирование и отправка сообщения в Telegram, но выполнение продолжается по частям.

    Returns:
        None
    """
    try:
        logger.info("📡 Инициализация GSpread клиента...")
        gs = get_gspread_client()

        sheet_name = sheets_names()['api_mywarehouse']

        logger.info(f"📥 Открытие таблицы-источника: {extract_table}")

        extract_table = tables_names()['wb_matrix_complete']
        extract_spreadsheet = gs.open(extract_table)
        extract_wsheet = extract_spreadsheet.worksheet(sheet_name)

        logger.info(f"📄 Чтение данных с листа: {sheet_name}")
        mywarehouse_data = extract_wsheet.get_all_values()

        mywarehouse_df = pd.DataFrame(
            mywarehouse_data[1:], columns=mywarehouse_data[0])

        logger.info(
            f"✅ Прочитано: {mywarehouse_df.shape[0]} строк, {mywarehouse_df.shape[1]} колонок")
    except Exception as e:
        msg = f"❌ Ошибка при чтении данных из '{sheet_name}': {e}"
        send_tg_message(msg)
        logger.exception(msg)

    try:

        logger.info(
            f"📦 Открытие таблицы для импорта: '{tables_names()['profit_supplier']}'")
        import_table = tables_names()['profit_supplier']

        import_spreadsheet = gs.open(import_table)

        if sheet_name in [ws.title for ws in import_spreadsheet.worksheets()]:

            logger.info(f"📄 Лист '{sheet_name}' найден — очищаю содержимое")

            import_worksheet = import_spreadsheet.worksheet(sheet_name)

            import_worksheet.clear()
            logger.info(f"🧹 Лист '{sheet_name}' успешно очищен")
        else:

            logger.info(f"🆕 Лист '{sheet_name}' не найден — создаю новый")

            import_worksheet = import_spreadsheet.add_worksheet(
                title=sheet_name,
                rows=mywarehouse_df.shape[0],
                cols=mywarehouse_df.shape[1]
            )
    except Exception as e:
        msg = f"❌ Ошибка при подготовке листа '{sheet_name}' в таблице '{import_table}': {e}"
        send_tg_message(msg)
        logger.exception(msg)

    try:
        logger.info(f"✅ Лист '{sheet_name}' создан")
        set_with_dataframe(
            import_worksheet,
            mywarehouse_df,
            row=1,
            col=1,
            include_column_header=True,
            include_index=False
        )

        logger.info(
            f"✅ Успешно загружено: {mywarehouse_df.shape[0]} строк, {mywarehouse_df.shape[1]} колонок в лист '{sheet_name}'")
    except Exception as e:
        msg = f"❌ Ошибка при загрузке данных в лист '{sheet_name}': {e}"
        send_tg_message(msg)
        logger.exception(msg)

    logger.info("🏁 Загрузка данных MyWarehouse завершена успешно.")


if __name__ == '__main__':

    upload_mywerehouse_in_gsheets()
# python -m scripts.integrations.split_and_upload_myWarehouse_sheets

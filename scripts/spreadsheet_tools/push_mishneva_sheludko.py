import pandas as pd
from scripts.utils.gspread_client import get_gspread_client
from scripts.utils.setup_logger import make_logger
from scripts.utils.config.factory import table_name_mirshik
from gspread_dataframe import set_with_dataframe

logger = make_logger(__name__, use_telegram=False)


def push_stocks_mishneva_sheludko(data: dict[str, pd.DataFrame], sheet_name: str = 'Остатки API'):
    gs = get_gspread_client()
    table_names = table_name_mirshik()

    group_map = {
        table_names[name]: df
        for name, df in data.items()
        if name in table_names
    }

    for table_title, df in group_map.items():
        logger.info(f"▶️ Обработка таблицы: {table_title}")

        try:

            spreadsheet = gs.open(table_title)
            logger.info(f"✅ Таблица открыта: {table_title}")

        except Exception as e:
            logger.exception(
                f"❌ Ошибка при открытии таблицы '{table_title}': {e}")
            continue

        try:

            worksheet = spreadsheet.worksheet(sheet_name)
            logger.info(f"📄 Лист найден: {sheet_name}")

        except Exception:
            logger.warning(f"⚠️ Лист '{sheet_name}' не найден. Создаю новый.")

            try:

                worksheet = spreadsheet.add_worksheet(
                    title=sheet_name, rows=1, cols=1)
                logger.info(f"✅ Лист создан: {sheet_name}")

            except Exception as e:
                logger.exception(
                    f"❌ Ошибка при создании листа '{sheet_name}' в таблице '{table_title}': {e}")
                continue

        try:

            logger.info(
                f"🧹 Очищаю лист '{sheet_name}' в таблице '{table_title}'")
            worksheet.clear()

        except Exception as e:
            logger.exception(
                f"❌ Ошибка при очистке листа '{sheet_name}' в таблице '{table_title}': {e}")
            continue

        try:

            logger.info(
                f"⬇️ Выгружаю DataFrame в '{sheet_name}' → '{table_title}'")
            set_with_dataframe(
                worksheet,
                df,
                col=1,
                row=1,
                include_column_header=True,
                include_index=False
            )
            logger.info(f"✅ Успешно выгружено: {table_title}")

        except Exception as e:
            logger.exception(
                f"❌ Ошибка при выгрузке данных в таблицу '{table_title}': {e}")

    logger.info('🎉 Все данные успешно обработаны и выгружены')

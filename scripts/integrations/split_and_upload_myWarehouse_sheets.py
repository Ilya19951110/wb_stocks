import pandas as pd
from scripts.utils.gspread_client import get_gspread_client
from scripts.utils.setup_logger import make_logger
from scripts.utils.config.factory import get_group_map, get_assortment_matrix_complete
from gspread_dataframe import set_with_dataframe


logger = make_logger(__name__, use_telegram=True)


def upload_mywerehouse_in_gsheets(sheet_name='API Мой склад') -> None:

    try:

        table_name = get_assortment_matrix_complete()
        gs = get_gspread_client()
        logger.info("🔑 GSpread клиент успешно инициализирован")

        spreadsheet = gs.open(table_name)
        worksheet = spreadsheet.worksheet(sheet_name)
        mywarehouse = worksheet.get_all_values()

        house_df = pd.DataFrame(mywarehouse[1:], columns=mywarehouse[0])

        logger.info(
            f"📥 Прочитано {len(house_df)} строк из '{sheet_name}' в '{table_name}'")

    except Exception:
        logger.exception(
            "❌ Ошибка при подключении к GSpread или чтении данных")
        return

    try:

        for table in get_group_map().keys():
            logger.info(f"📂 Работа с таблицей: '{table}'")
            sh = gs.open(table)

            work_sheets = [ws.title for ws in sh.worksheets()]
            logger.info(
                f"🔎 Проверяю наличие листа: '{sheet_name}' в таблице '{table}'")

            if sheet_name in work_sheets:
                logger.info("✅ Лист найден — обновляю размер под DataFrame")
                wsheet = sh.worksheet(sheet_name)

                wsheet.resize(
                    rows=house_df.shape[0],
                    cols=house_df.shape[1]
                )
            else:
                logger.warning("🆕 Лист не найден — создаю новый")
                wsheet = sh.add_worksheet(
                    title=sheet_name, rows=house_df.shape[0], cols=house_df.shape[1])

    except Exception:
        logger.exception("💥 Ошибка при обработке таблиц из get_group_map")

    try:
        logger.info("🧼 Очищаю лист перед загрузкой")
        wsheet.clear()

        logger.info("📤 Загружаю данные в Google Sheet...")
        set_with_dataframe(
            wsheet,
            house_df,
            row=1,
            col=1,
            include_column_header=True,
            include_index=False
        )

        logger.info(f"🎉 Успешно загружено в таблицу: '{table}'")

    except Exception:
        logger.exception("❗ Ошибка при очистке или загрузке данных на лист")

    logger.info("✅ Все данные успешно загружены! 🟢")


upload_mywerehouse_in_gsheets()
# python -m scripts.integrations.split_and_upload_myWarehouse_sheets

import pandas as pd
from collections import defaultdict
from scripts.utils.setup_logger import make_logger
from scripts.utils.gspread_client import get_gspread_client
from gspread_dataframe import set_with_dataframe
from scripts.utils.config.factory import get_group_map

logger = make_logger(__name__, use_telegram=True)


def update_barcode(data: dict[str, tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]],
                   sheet_name='API WB barcode', clear_range: list[str] = None) -> None:

    MAP = get_group_map()
    grouped_df = defaultdict(pd.DataFrame)

    if clear_range is None:
        clear_range = ['A:C']
    try:
        logger.info('🔌 Подключаюсь к Google Sheets клиенту...')
        gs = get_gspread_client()

        logger.info('✅ Подключение к Google Sheets установлено')
    except Exception:
        logger.exception("❌ Ошибка при подключении к Google Sheets")
        return

    for name, (_, _, barcode) in data.items():
        for sheet, people in MAP.items():
            if name in people:
                grouped_df[sheet] = pd.concat(
                    [grouped_df[sheet], barcode], ignore_index=True)

    logger.info("📊 Данные баркодов сгруппированы по таблицам")

    # === Загрузка данных в таблицы ===
    for sheet, df_barcode in grouped_df.items():
        try:
            logger.info(f'📂 Открываю таблицу: "{sheet}"')
            spreadsheet = gs.open(sheet)

            logger.info(f'📄 Открываю лист: "{sheet_name}"')
            worksheet = spreadsheet.worksheet(sheet_name)

            logger.info(
                f'🧼 Очищаю диапазон {clear_range} в листе "{sheet_name}"')
            worksheet.batch_clear(clear_range)

            logger.info(
                f'⬆️ Загружаю {len(df_barcode)} строк в лист "{sheet_name}" таблицы "{sheet}"')
            set_with_dataframe(
                worksheet,
                df_barcode,
                col=1,
                row=1,
                include_column_header=True,
                include_index=False
            )

            logger.info(
                f'✅ Данные успешно загружены в "{sheet}" → "{sheet_name}"')
        except Exception:
            logger.exception(
                f"❌ Ошибка при загрузке данных в таблицу '{sheet}' (лист '{sheet_name}')")

    logger.info("🏁 Загрузка всех баркодов завершена")

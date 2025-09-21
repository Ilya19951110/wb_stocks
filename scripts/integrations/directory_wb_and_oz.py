"""
📊 Ассортиментная матрица Ozon + WB — Автоматическая выгрузка

Этот скрипт выполняет интеграцию с Google Sheets для обновления данных из справочников по Ozon и Wildberries.
Он подгружает нужные листы, фильтрует по ИП, и загружает информацию в итоговые таблицы для разных предпринимателей.

────────────────────────────────────────────────────────────────────────────

📦 Используемые библиотеки:
    - gspread             → для работы с Google Sheets API
    - pandas              → для обработки табличных данных
    - colorlog, logging   → для цветных логов и отладки
    - gspread_dataframe   → для загрузки pandas DataFrame в Google Sheets

────────────────────────────────────────────────────────────────────────────

🔁 Общий процесс:

1. Получение данных из таблиц:
   - 'Ассортиментная матрица. Полная' (WB)
   - 'Ассортиментная матрица OZON' (Ozon)

2. Формирование таблиц по ИП:
   - Фильтрация по колонке "ИП"
   - Сбор справочников и баркодов

3. Загрузка в итоговые Google-таблицы:
   - Очищаются существующие диапазоны
   - Загружаются новые данные в три листа:
       • Справочник OZ
       • Справочник WB
       • Баркода OZ

────────────────────────────────────────────────────────────────────────────

🧾 Структура table_entrepreneur:

    {
        'Фин модель Иосифовы Р А М': ('Gabriel', ['Рахель', 'Михаил', 'Азарья']),
        'Фин модель Галилова':       ('Havva',   ['Галилова']),
        'Фин модель Мартыненко':     ('Ucare',   ['Мартыненко']),
    }

    Ключ — имя целевой итоговой таблицы
    Значение — кортеж: (значение для фильтрации в Ozon, список ИП для фильтрации WB)

────────────────────────────────────────────────────────────────────────────

🖇️ Функции:

▪ request_oz_and_wb_product_range_matrix()
    → Возвращает: словарь таблиц + параметры листов + клиент gspread

▪ upload_to_sheet(data_dict, sheet_directory_oz, worksheet_barcode_oz, sheet_directory_wb, gs)
    → Загружает справочники и баркоды в целевые таблицы

────────────────────────────────────────────────────────────────────────────

📌 Требования:
    - Созданный и привязанный Google Service Account
    - Валидный key.json
    - Доступ к таблицам через e-mail сервисного аккаунта
    - Наличие столбца "ИП" в исходных таблицах

────────────────────────────────────────────────────────────────────────────

💡 Советы по улучшению:
    - Добавить Telegram-уведомления о статусе выполнения
    - Сохранять лог в отдельный лог-файл или лист Google
    - Вынести table_entrepreneur в YAML/JSON
    - Встроить в GitHub Actions по расписанию

────────────────────────────────────────────────────────────────────────────

🔧 Вспомогательные зависимости:

▪ get_gspread_client()
    → Из модуля scripts.gspread_client
    → Отвечает за авторизацию в Google Sheets API через service account

────────────────────────────────────────────────────────────────────────────

🧑‍💻 Автор: Илья
"""
from scripts.utils.prepare_values_df import prepare_values_for_sheets
from scripts.utils.config.factory import get_client_info, tables_names, sheets_names
from scripts.utils.gspread_client import get_gspread_client
from scripts.utils.telegram_logger import send_tg_message
from scripts.utils.setup_logger import make_logger
from datetime import datetime
import pandas as pd
import gspread
from gspread.utils import rowcol_to_a1

logger = make_logger(__name__, use_telegram=False)


def request_oz_and_wb_product_range_matrix() -> tuple[dict[str, pd.DataFrame], str, str, gspread.client.Client]:

    gs = get_gspread_client()

    table_entrepreneur = get_client_info()['finmodel_map']

    wb_matrix = tables_names()['wb_matrix_complete']
    oz_matrix = tables_names()['oz_matrix_complete']

    # upload_worksheet_directory_oz
    spreadsheet_wb = gs.open(wb_matrix)
    spreadsheet_oz = gs.open(oz_matrix)

    sheet_directory_wb = sheets_names()['directory_wb']
    sheet_directory_oz = sheets_names()['directory_oz']
    worksheet_barcode_oz = sheets_names()['barcodes_oz']

    wb_directory_worksheet = spreadsheet_wb.worksheet(
        sheet_directory_wb)

    oz_directory_worksheet = spreadsheet_oz.worksheet(
        sheet_directory_oz)

    oz_directory_barcode = spreadsheet_oz.worksheet(
        worksheet_barcode_oz)

    get_date_directory_wb = wb_directory_worksheet.get_all_values()
    get_date_directory_oz = oz_directory_worksheet.get_all_values()
    get_date_barcode_oz = oz_directory_barcode.get_all_values()

    # преобразуем из таблицы Ассортиментная матрица. Полная из листа Справочник WB
    df_directory_wb = pd.DataFrame(
        get_date_directory_wb[1:], columns=get_date_directory_wb[0]
    )

    # преобразуем данные из таблицы Ассортиментная матрица OZON из листа Справочник OZ
    df_directory_oz = pd.DataFrame(
        get_date_directory_oz[1:], columns=get_date_directory_oz[0])

    # преобразуем Ассортиментная матрица OZON Баркода OZ
    df_barcode = pd.DataFrame(
        get_date_barcode_oz[1:], columns=get_date_barcode_oz[0])

    if "ИП" not in df_directory_oz.columns or "ИП" not in df_directory_wb.columns:
        msg = "❌ В таблице нет столбца 'ИП'"
        send_tg_message(msg)
        raise ValueError(msg)

    res = {
        table: (df_directory_oz[df_directory_oz['ИП'] == oz_names],
                df_directory_wb[df_directory_wb['ИП'].isin(wb_name)],
                df_barcode
                )
        for table, (oz_names, wb_name) in table_entrepreneur.items()
    }

    return res, sheet_directory_oz, worksheet_barcode_oz, sheet_directory_wb, gs


def upload_to_sheet(data_dict: dict[str, tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]],
                    sheet_directory_oz: str, worksheet_barcode_oz: str, sheet_directory_wb: str, gs: gspread.client.Client) -> None:

    for table, (oz_directory, wb_directory, barcode) in data_dict.items():
        try:
            logger.info(f"Пробую открыть таблицу: {table}")
            # открываем таблицу
            upload_spreadsheet = gs.open(table)

            # открываем лист Справочник OZ
            upload_worksheet_directory_oz = upload_spreadsheet.worksheet(
                sheet_directory_oz)
            # открываем лист Справочник WB
            upload_worksheet_directory_wb = upload_spreadsheet.worksheet(
                sheet_directory_wb)
            # открываем лист Баркода OZ
            upload_worksheet_barcode = upload_spreadsheet.worksheet(
                worksheet_barcode_oz)

        except Exception as e:
            msg = f'Ошибка подключения к: {table}\n{e}'
            send_tg_message(msg)
            logger.error(msg)

        try:
            row, col = oz_directory.shape
            end_cell = rowcol_to_a1(row + 1, col)
            logger.debug(f"Справочник OZ end_cell: {end_cell}".upper())

            upload_worksheet_directory_oz.batch_clear([f"A2:{end_cell}"])

           
            logger.info(f'Выгружаю Справочник OZ в гугл таблицу: {table}')

            upload_worksheet_directory_oz.update(
                values=prepare_values_for_sheets(oz_directory),
                value_input_option="USER_ENTERED",
                range_name='A2'
            )

            logger.info(f"✅ Данные успешно загружены в: {table}")

        except Exception as e:
            msg = f'❌ Ошибка при выгрузке данных в {table}: {e}'
            send_tg_message(msg)
            logger.error(msg)

        try:
            logger.info(f'Выгружаю данные в лист Справочник WB: {table}')
            row, col = wb_directory.shape
            end_cell = rowcol_to_a1(row + 1, col)
            logger.debug(f"Справочник WB end_cell: {end_cell}".upper())

            upload_worksheet_directory_wb.batch_clear([f"A2:{end_cell}"])

            upload_worksheet_directory_wb.update(
                values=prepare_values_for_sheets(wb_directory),
                value_input_option="USER_ENTERED",
                range_name="A2"
            )

            logger.info(f'Справочник WB выгружен в таблицу: {table}')
        except Exception as e:
            msg = f"❌ Ошибка при выгрузке 'Справочник WB' в {table}: {e}"
            send_tg_message(msg)
            logger.error(msg)

        try:
            row, col = barcode.shape
            end_cell = rowcol_to_a1(row + 1, col)
            logger.info(f'Очищаю диапозон листа: {worksheet_barcode_oz}')

            upload_worksheet_barcode.batch_clear([f"A2:{rowcol_to_a1(f"A2:{end_cell}", 3)}"])

            logger.info(
                f'Очищен диапозон: {end_cell} в {table}, лист {worksheet_barcode_oz}')

            logger.info(f'Выгружаю Баркода OZ в гугл таблицу: {table}')

            upload_worksheet_barcode.update(
                [barcode.columns.tolist()] +
                prepare_values_for_sheets(barcode),
                value_input_option="USER_ENTERED",
                range_name='A2'
            )

            logger.info(
                f"✅ Данные успешно загружены в лист {worksheet_barcode_oz} таблицы {table}")

        except Exception as e:
            msg = f'❌ Ошибка при выгрузке данных в {table}: {e}'
            send_tg_message(msg)
            logger.error(msg)

    logger.info("🎉 Все данные загружены!")


if __name__ == '__main__':
    send_tg_message(
        f"🏁 Скрипт запущен 'req_directory_wb_and_oz': {datetime.now():%Y-%m-%d %H:%M:%S}")

    result_dict, oz_sheet, oz_barcode_sheet, wb_sheet, gs = request_oz_and_wb_product_range_matrix()

    upload_to_sheet(
        data_dict=result_dict,
        sheet_directory_oz=oz_sheet,
        worksheet_barcode_oz=oz_barcode_sheet,
        sheet_directory_wb=wb_sheet,
        gs=gs
    )
# C:\Users\Ilya\OneDrive\Рабочий стол\Iosifovy\ py -m scripts.integrations.directory_wb_and_oz

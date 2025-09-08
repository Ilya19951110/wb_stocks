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
from gspread_dataframe import set_with_dataframe
from datetime import datetime
import pandas as pd
import gspread


logger = make_logger(__name__, use_telegram=False)


def request_oz_and_wb_product_range_matrix() -> tuple[dict[str, pd.DataFrame], str, str, gspread.client.Client]:
    """
📊 Ассортиментная матрица: интеграция OZON + WB → Google Sheets

Этот модуль автоматически выгружает отфильтрованные справочники по ИП из двух ассортиментных матриц:
- Wildberries (WB)
- Ozon (OZ)

Результаты загружаются в итоговые Google-таблицы для каждого предпринимателя, указанных в `get_finmodel_to_cabinet_map()`.

────────────────────────────────────────────────────────────────────────────

📌 Основные функции:

▪ request_oz_and_wb_product_range_matrix() → tuple
    Получает и фильтрует таблицы WB и OZ по ИП. Возвращает:
    - Словарь: {имя итоговой таблицы: (df_ozon, df_wb, df_barcodes)}
    - Названия листов: directory_oz, barcodes_oz, directory_wb
    - Авторизованный gspread клиент

▪ upload_to_sheet(data_dict, sheet_directory_oz, worksheet_barcode_oz, sheet_directory_wb, gs)
    Загружает полученные датафреймы в соответствующие листы итоговых таблиц.
    Предварительно очищает старые данные (начиная с B1, кроме barcodes).

────────────────────────────────────────────────────────────────────────────

📦 Используемые библиотеки:
- pandas                → обработка табличных данных
- gspread               → взаимодействие с Google Sheets API
- gspread_dataframe     → выгрузка pandas.DataFrame в Google Sheet
- colorlog, logging     → логирование в терминал/файл
- custom utils:
    - get_gspread_client → авторизация в Sheets API
    - send_tg_message    → отправка логов в Telegram
    - get_assortment_matrix_complete / OZON
    - get_finmodel_to_cabinet_map → список целевых таблиц и фильтров
    - sheets_names       → названия рабочих листов

────────────────────────────────────────────────────────────────────────────

🧾 Структура table_entrepreneur:

Пример:
{
    'Фин модель Иосифовы Р А М': ('Gabriel', ['Рахель', 'Михаил', 'Азарья']),
    'Фин модель Галилова':       ('Havva', ['Галилова']),
}

→ Ключ: название итоговой таблицы
→ Значение: кортеж (фильтр по OZON, фильтры по WB)

────────────────────────────────────────────────────────────────────────────

⚙️ Запуск:

    $ python -m scripts.integrations.directory_wb_and_oz

→ Отправит уведомление в Telegram, выполнит обновление всех целевых таблиц по списку.

────────────────────────────────────────────────────────────────────────────

🧑‍💻 Автор: Илья
📅 Версия: Июль 2025
"""
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
            # получаем максимальное количество доступных строк и столбцов в листе Справочник OZ
            # sheet_rows_directory = upload_worksheet_directory_oz.row_count
            # sheet_cols_directory = upload_worksheet_directory_oz.col_count

            # очищаем диапозон с ячейки b1
            # if sheet_cols_directory > 1:
            #     clear_range = f"B1:{gspread.utils.rowcol_to_a1(sheet_rows_directory, sheet_cols_directory)}"
            #     upload_worksheet_directory_oz.batch_clear([clear_range])
            #     logger.info(
            #         f"🧹 Очищен диапазон: {clear_range} в {table}, лист {sheet_directory_oz}")

            # else:
            # logger.warning(
            #     "ℹ️ В листе только столбец A — ничего не удалено.")

            upload_worksheet_directory_oz.clear()
            logger.info(f'Выгружаю Справочник OZ в гугл таблицу: {table}')

            upload_worksheet_directory_oz.update(
                [oz_directory.columns.tolist()] +
                prepare_values_for_sheets(oz_directory),
                value_input_option="USER_ENTERED"
            )

            logger.info(f"✅ Данные успешно загружены в: {table}")

        except Exception as e:
            msg = f'❌ Ошибка при выгрузке данных в {table}: {e}'
            send_tg_message(msg)
            logger.error(msg)

        try:
            logger.info(f'Выгружаю данные в лист Справочник WB: {table}')
            upload_worksheet_directory_wb.clear()

            upload_worksheet_directory_wb.update(
                [wb_directory.columns.tolist()] +
                prepare_values_for_sheets(wb_directory),
                value_input_option="USER_ENTERED"
            )

            logger.info(f'Справочник WB выгружен в таблицу: {table}')
        except Exception as e:
            msg = f"❌ Ошибка при выгрузке 'Справочник WB' в {table}: {e}"
            send_tg_message(msg)
            logger.error(msg)

        try:
            logger.info(f'Очищаю диапозон листа: {worksheet_barcode_oz}')
            clear_range_barcode = f"A1:{gspread.utils.rowcol_to_a1(upload_worksheet_barcode.row_count, 3)}"
            upload_worksheet_barcode.batch_clear([clear_range_barcode])

            logger.info(
                f'Очищен диапозон: {clear_range_barcode} в {table}, лист {worksheet_barcode_oz}')

            logger.info(f'Выгружаю Баркода OZ в гугл таблицу: {table}')

            upload_worksheet_barcode.update(
                [barcode.columns.tolist()] +
                prepare_values_for_sheets(barcode),
                value_input_option="USER_ENTERED"
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

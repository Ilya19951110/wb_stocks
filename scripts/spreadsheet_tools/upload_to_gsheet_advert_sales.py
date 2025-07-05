"""
    📤 Сохраняет сгруппированные данные в соответствующие Google Таблицы.

    Данные из разных кабинетов (по ключу `dict_data`) группируются по общим итоговым таблицам
    согласно карте `GROUP_MAP`, а затем выгружаются в указанный лист Google Таблицы (worksheet_name).

    Параметры:
    ----------
    dict_data : Dict[str, pd.DataFrame]
        Словарь, где ключ — имя кабинета, значение — соответствующий DataFrame с данными для выгрузки.

    worksheet_name : str
        Имя листа в Google Таблице, в который будут загружены данные.

    Карта группировки:
    ------------------
    GROUP_MAP определяет, какие кабинеты (имена) объединяются в какую итоговую таблицу:
        {
            'Фин модель Иосифовы Р А М': ['Азарья', 'Рахель', 'Михаил'],
            'Фин модель Галилова': ['Галилова'],
            'Фин модель Мартыненко и Торгмаксимум': ['Мартыненко', 'Торгмаксимум']
        }

    Процесс:
    --------
    1. Данные из dict_data группируются в соответствии с GROUP_MAP.
    2. Для каждой итоговой таблицы:
        - Таблица открывается через Google Sheets API.
        - Если данных больше текущих размеров листа, он расширяется.
        - Данные добавляются после последней строки.

    Возвращает:
    -----------
    None

    Зависимости:
    ------------
    - gspread
    - pandas
    - gspread_client.get_gspread_client()

    Пример:
    -------
        save_in_gsh(dict_data=результаты_по_кабинетам, worksheet_name="Статистика")
    """

from scripts.utils.config.factory import get_group_map
from scripts.utils.gspread_client import get_gspread_client
import pandas as pd


def save_in_gsh(dict_data: dict[str, pd.DataFrame], worksheet_name: str) -> None:

    def goup_by_sheet(data: dict[str, pd.DataFrame], MAP: dict[str, list[str]]) -> dict[str, pd.DataFrame]:
        result = {}

        for table, people in MAP.items():
            res = [data[name] for name in people if name in data]

            if not res:
                print(f'⚠️ {table}: нет данных для выгрузки')
                continue

            result[table] = pd.concat(res, ignore_index=True)
        print('🚀🚀 Данные сгруппированы!')
        return result

    def update_sheet(group: dict[str, pd.DataFrame], worksheet_name: str) -> None:
        gs = get_gspread_client()

        for name, df in group.items():
            sh = gs.open(name)
            worksheet = sh.worksheet(worksheet_name)

            existing = worksheet.get_all_values()
            start_row = len(existing) + 1 if existing else 1

            current_rows = worksheet.row_count
            current_cols = worksheet.col_count

            req_rows = len(df) + start_row
            req_cols = df.shape[1]

            if req_cols > current_cols or req_rows > current_rows:

                worksheet.resize(
                    rows=max(req_rows, current_rows),
                    cols=max(req_cols, current_cols)
                )

            print(
                f"📤 Кабинет {name} Добавляю {len(df)} строк в таблицу '{worksheet_name}' начиная с A{start_row}")

            worksheet.update(
                range_name=f"A{start_row}",
                values=df.values.tolist())

        print('📤 Данные выгружены в гугл таблицу!🚀🚀')

    grouped = goup_by_sheet(data=dict_data, MAP=get_group_map())
    update_sheet(grouped, worksheet_name=worksheet_name)

from gspread_dataframe import set_with_dataframe
from gspread_formatting import *

import time
import gspread
import pandas as pd


def import_export_in_gh():
    data = {}
    col_df = ['Сезон',	'% выкупа', 'Себестоимость', 'Литраж', 'Название', 'Бренд',  'Предмет',	'Артикул', 'продавца',	'плановый', 'ДРР в заказ',
              'Окончательная цена',	'Группа',	'рентабельность',	'Реклама в покупку', 'quotient(Затраты на логистикуЦена макс)',	'quotient(Затраты на хранениеЦена макс)',
              'quotient(СебестоимостьЦена макс)', 'Опер расходы']
    # Подключился к сервисному акаунту
    gs = gspread.service_account(
        filename='key.json')
    # Подключился к таблице
    spreadsheet = gs.open('Ассортиментная матрица. Полная')
    # Подключение к нужному листу
    worksheets = spreadsheet.worksheet('Справочник WB')
    # Получил все данные из таблицы 'Ассортиментная матрица. Полная', листа 'Справочник WB'
    all_data = worksheets.get_all_values()[2:]

    # Использовал для теста, получил заголовки
    gs_headres = worksheets.row_values(1)
    # print(col_in_gsh)

    # Создал датафрейм
    all_df = pd.DataFrame(all_data, columns=col_df)
    print(all_df.columns.tolist())

    num_col = ['Артикул WB', 'Сумма остатков', 'ID КТ',
               'Остатки с тем что в пути', 'Себестоимость', 'Литраж', 'Окончательная цена']

    num_float = ['% выкупа', 'плановый ДРР в заказ', 'рентабельность', 'Реклама в покупку',
                 'quotient(Затраты на логистикуЦена макс)', 'quotient(Затраты на хранениеЦена макс)', 'quotient(СебестоимостьЦена макс)', 'Опер расходы']

    all_df[num_col] = all_df[num_col].replace('', 0).astype(int)

    for col in num_float:
        all_df[col] = pd.to_numeric(
            all_df[col].astype(str).str.replace(',', '.', regex=False),
            errors='coerce'
        )

 # данные для словаря, где Ключ - название таблицы, значение - датафрейм для выгрузки отфильтрованный по ИП
    data['Фин модель Иосифовы Р А М'] = all_df[all_df['ИП'].isin(
        ['Рахель', 'Азарья', 'Михаил'])]

    data['Фин модель Галилова'] = all_df[all_df['ИП'] == 'Галилова']

    data['Фин модель Мартыненко и Торгмаксимум'] = all_df[all_df['ИП'].isin(
        ['Торгмаксимум', 'Мартыненко'])]

    for name, df in data.items():
        # открываем нужную тадицу
        spr_sheets = gs.open(name)
        # получаем нужный лист
        wks_sheets = spr_sheets.worksheet('Справочник WB')

        # if gs_headres != df.columns.tolist():
        #     print(f"Ошибка: заголовки в таблице {name} не совпадают")
        #     break

        wks_sheets.clear()
        set_with_dataframe(wks_sheets, df)

        print(f"Данные выгружены в таблицу {name}\nдлина: {len(df)}")

    print('готово')


if __name__ == "__main__":
    begin = time.time()
    import_export_in_gh()
    end = time.time()
    print(f'время выполнения: {end-begin}')

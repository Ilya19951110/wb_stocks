import pandas as pd
from scripts.utils.setup_logger import make_logger
from typing import Any, Dict

logger = make_logger(__name__, use_telegram=True)


def get_current_week_sales_df(sales: pd.DataFrame, ID: pd.DataFrame, name: str) -> pd.DataFrame:

    def read_to_json(data: dict[str, Any], parent_key='', sep='_') -> Dict[str, Any]:

        items = []

        for k, v in data.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(read_to_json(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))

        return dict(items)

    data = [read_to_json(item) for item in sales]
    df = pd.DataFrame(data)

    assert isinstance(df, pd.DataFrame), "Входной df должен быть DataFrame"

    columns_rus = [
        'Артикул WB', 'Артикул поставщика', 'Бренд', 'ID категории', 'Название категории', 'Начало текущего периода', 'Конец текущего периода', 'Просмотры карточки', 'Добавления в корзину',
        'Количество заказов', 'Сумма заказов (руб)', 'Количество выкупов', 'Сумма выкупов (руб)', 'Количество отмен', 'Сумма отмен (руб)', 'Среднее заказов в день', 'Средняя цена (руб)',
        'Конверсия в корзину (%)', 'Конверсия в заказ (%)', 'Конверсия в выкуп (%)', 'Начало предыдущего периода', 'Конец предыдущего периода', 'Просмотры карточки (пред.)', 'Добавления в корзину (пред.)',
        'Количество заказов (пред.)', 'Сумма заказов (пред., руб)', 'Количество выкупов (пред.)', 'Сумма выкупов (пред., руб)', 'Количество отмен (пред.)', 'Сумма отмен (пред., руб)', 'Среднее заказов в день (пред.)',
        'Средняя цена (пред., руб)', 'Конверсия в корзину (пред., %)', 'Конверсия в заказ (пред., %)', 'Конверсия в выкуп (пред., %)', 'Динамика просмотров (%)', 'Динамика корзины (%)',
        'Динамика заказов (%)', 'Динамика суммы заказов (%)', 'Динамика выкупов (%)', 'Динамика суммы выкупов (%)', 'Динамика отмен (%)', 'Динамика суммы отмен (%)', 'Динамика ср. заказов в день (%)',
        'Динамика ср. цены (%)', 'Изменение конверсии в корзину', 'Изменение конверсии в заказ', 'Изменение конверсии в выкуп', 'Остатки на маркетплейсе', 'Остатки на WB'
    ]

    df = df.rename(
        columns=dict(zip(df.columns.tolist(), columns_rus))
    )

    date_col = ['Начало текущего периода', 'Конец текущего периода',
                'Начало предыдущего периода', 'Конец предыдущего периода']

    df[date_col] = df[date_col].apply(
        pd.to_datetime, errors='coerce').apply(lambda x: x.dt.date)

    current_week_col = df.columns[:df.columns.get_loc(
        'Начало предыдущего периода')].tolist() + df.columns[-3:].tolist()

    current_week = df[current_week_col].copy()

    current_week['Начало текущего периода'] = pd.to_datetime(
        current_week['Начало текущего периода'])

    current_week['Номнед'] = current_week['Начало текущего периода'].dt.isocalendar().week

    current_week['Начало текущего периода'] = pd.to_datetime(
        current_week['Начало текущего периода']).dt.date

    final_df = pd.merge(
        current_week,
        ID,
        left_on='Артикул WB',
        right_on='Артикул WB',
        how='left',
        indicator=True
    )

    final_df['Кабинет'] = name
    final_df['ID KT'] = final_df['ID KT'].fillna(0)

    final_df = final_df.drop_duplicates()
    # ID KT
    final_df = final_df.rename(columns={
        'ID KT': 'ID',
        'Номнед': 'Неделя',
        'Просмотры карточки': 'Переходы в карточку',
        'Добавления в корзину': 'Положили в корзину',
        'Количество заказов': 'Заказали, шт',
        'Количество выкупов': 'Выкупили, шт',
        'Количество отмен': 'Отменили, шт',
        'Сумма заказов (руб)': 'Заказали на сумму, руб',

    }).filter([
        'ID', 'Неделя', 'Переходы в карточку',	'Положили в корзину',	'Заказали, шт',	'Выкупили, шт', 'Отменили, шт',	'Заказали на сумму, руб',
    ]).groupby([
        'ID', 'Неделя',
    ]).agg({
        'Переходы в карточку': 'sum',
        'Положили в корзину': 'sum',
        'Заказали, шт': 'sum',
        'Выкупили, шт': 'sum',
        'Отменили, шт': 'sum',
        'Заказали на сумму, руб': 'sum',
    }).reset_index()

    logger.info(final_df.head(5))

    return final_df

from scripts.utils.setup_logger import make_logger
import pandas as pd
import numpy as np


logger = make_logger(__name__, use_telegram=True)


def group_advert_and_id(camp_df: pd.DataFrame, ID: pd.DataFrame, name: str) -> pd.DataFrame:
    """_summary_
    функция объединяет (merge) и группирует (groupby) два датафрейма, после фильтрует их по столбцам

    Args:
        camp_df (_type_): DataFrame рекламной кампании за определенный период

        ID (_type_): DataFrame возвращается из функции get_cards() которая находится в файле test.py, которая возвращает все созданные карточки
                     товаров с idkt (idkt - это id склейки артикулов wb)

        name (_type_): имя кабинета, которое используется для логирования выполнения кода

    Returns:
        _type_: возвращает DataFrame отфильтрованный по столбцам
    """

    ID['updatedAt'] = pd.to_datetime(ID['updatedAt'])

    latest_idkt = (
        ID.sort_values('updatedAt').drop_duplicates(
            subset='Артикул WB', keep='last').reset_index(drop=True)
    )
    camp_df['date'] = pd.to_datetime(camp_df['date']).dt.date

    camp_df['Неделя'] = pd.to_datetime(camp_df['date']).dt.isocalendar().week
    camp_df = camp_df.rename(columns={'sum': 'expenses'})

    camp_df.drop(columns=['date'], inplace=True)

    logger.info(
        f"{name}💰 Сумма ДО merge: {camp_df['expenses'].sum():,.2f} ₽\033[0m\n\033[93m🔍 Строк до merge: {len(camp_df)}")

    camp_df = pd.merge(
        camp_df.rename(columns={'nmId': 'Артикул WB'}),
        latest_idkt.rename(columns={'ID KT': 'ID'}),
        left_on='Артикул WB',
        right_on='Артикул WB',
        how='left'
    )

    logger.info(
        f"💥 Сумма ПОСЛЕ merge: {camp_df['expenses'].sum():,.2f} ₽\033[0m\n\033[93m🔍 Строк после merge: {len(camp_df)}")

    camp_df['ID'] = pd.to_numeric(
        camp_df['ID'], errors='coerce').fillna(0).astype(int)

    result = camp_df.groupby(['ID', 'Неделя', 'Артикул WB']).agg({
        'views': 'sum',
        'clicks': 'sum',
        'atbs': 'sum',
        'orders': 'sum',
        'shks': 'sum',
        'sum_price': 'sum',
        'expenses': 'sum'
    }).reset_index()

    result = result.drop_duplicates()

    result = result.rename(columns={
        'views': 'Просмотры',
        'clicks': 'Переходы',
        'atbs': 'Добавления в корзину',
        'orders': 'Количество заказов',
        'shks': 'Количество заказанных товаров',
        'sum_price': 'Сумма заказов',
        'expenses': 'Расход,Р'
    })

    result['CTR'] = np.where(
        result['Просмотры'] == 0,
        0,
        (
            result['Переходы'] / result['Просмотры']
        ).round(3)
    )

    result = result.filter(['ID', 'Неделя', 'Расход,Р', 'Артикул WB', 'CTR'])

    logger.info(
        f"🎯 Агрегация по ID выполнена для {name}!\n {result['Расход,Р'].sum():,.2f}"
    )

    return result

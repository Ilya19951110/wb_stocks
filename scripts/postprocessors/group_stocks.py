from scripts.utils.telegram_logger import send_tg_message
from scripts.utils.setup_logger import make_logger
import pandas as pd

logger = make_logger(__name__, use_telegram=False)


def merge_and_transform_stocks_with_idkt(stocks: pd.DataFrame, IDKT: pd.DataFrame, name: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    📦 Объединение остатков и справочника карточек WB с последующей агрегацией, очисткой и трансформацией.

    Функция объединяет таблицу остатков `stocks` с таблицей карточек `IDKT` по ключам `Артикул WB` и `Баркод`.
    После объединения приводит данные к единому формату, удаляет дубли, агрегирует остатки и готовит два итоговых
    датафрейма:
    - `result` — основная таблица остатков с ID KT и характеристиками;
    - `seller_article` — таблица соответствия `Артикул WB` ↔ `Баркод` ↔ `Артикул поставщика`.

    ────────────────────────────────────────────────────────────

    🔧 Параметры:
    -------------
    stocks : pd.DataFrame
        Таблица остатков товаров по артикулам WB и баркодам. Содержит поля вроде:
        - 'Артикул WB', 'Баркод', 'Размер', 'Цена', 'Скидка', 'Итого остатки' и др.

    IDKT : pd.DataFrame
        Таблица карточек товаров с `ID KT`, габаритами, фото, наименованиями и баркодами.

    name : str
        Имя кабинета (для логов и Telegram-уведомлений).

    📤 Возвращает:
    --------------
    Tuple[pd.DataFrame, pd.DataFrame]
        - Первый датафрейм `result`: агрегированная таблица остатков с `ID KT`, ценами, габаритами и т.п.
        - Второй датафрейм `seller_article`: таблица сопоставлений `Артикул WB`, `Баркод`, `Артикул поставщика`, `Размер`.

    🧪 Основные шаги обработки:
    ---------------------------
    1. Приведение типов (`int`, `str`) для нужных колонок.
    2. Merge по ['Артикул WB', 'Баркод'] с outer join.
    3. Очистка мусора: удаление лишних колонок, префиксов, NaN.
    4. Группировка и агрегация остатков.
    5. Расчёт `Цена до СПП`.
    6. Выделение `seller_article`.
    7. Финальное форматирование и сортировка.

    ⚠️ Особенности:
    ---------------
    - При наличии карточек, которые есть только в таблице остатков (`right_only`), они логируются как косячные.
    - Пропущенные значения в строковых колонках заменяются на '-'.
    - В Telegram отправляется сообщение при ошибках.
    - Проверяется наличие дубликатов в финальной таблице `result`.

    🧠 Автор: Илья  
    🗓 Версия: Июль 2025
    """

    send_tg_message(
        f"🚀 Запущена функция merge_and_transform_stocks_with_idkt для кабинета: {name}")
    try:
        logger.info(
            "📊 Приводим типы данных в столбцах [Артикул WB, Баркод, Размер, ID KT]...")

        type_map = {
            'Артикул WB': int,
            'Баркод': str,
            'Размер': str
        }
        for df in [stocks, IDKT]:
            for col, dtype in type_map.items():
                df[col] = df[col].astype(dtype)

        IDKT['ID KT'] = IDKT['ID KT'].astype(int)

        logger.info("✅ Типы данных успешно приведены!")
    except Exception as e:
        msg = f"❌ Не удалось привести типы данных {name}: {e}"
        send_tg_message(msg)
        logger.error(msg)
        # Объединяем две таблицы остатки цепляем к idkt
    try:
        logger.info("🔗 Выполняем объединение таблиц (merge)...")

        result = pd.merge(
            IDKT,
            stocks,
            on=['Артикул WB', 'Баркод'],
            how='outer',
            indicator=True,
            suffixes=('_IDKT', '_stocks')
        )
        logger.info("✅ Объединение выполнено успешно!")

    except Exception as e:
        msg = f"❌ Не удалось объединить таблицы: {e}"
        send_tg_message(msg)
        logger.error(msg)

    try:
        logger.info("🧹 Начинаем финальную очистку и обработку данных...")
        # Удаляем не нужные столбцы
        result = result.drop(columns=[col for col in result.columns if col.endswith('_stocks')]+['Дата Обновления', 'warehouseName',
                                                                                                 'quantity', 'inWayToClient', 'inWayFromClient',
                                                                                                 'category', 'subject', 'isRealization', 'SCCode', 'isSupply'], errors='ignore')

        # удаляем суффиксы _IDKT у столбцов, которые остались
        result.columns = [
            col.replace('_IDKT', '') for col in result.columns
        ]

        # выбираем столбцы
        num_col = ['Цена', 'Скидка',
                   'Итого остатки', 'Ширина', 'Высота', 'Длина']
        string_cols = ['Бренд', 'Размер', 'Категория', 'Наименование',
                       ]

        # Заполняем NAN в Цена и Скидка последними известными знач для артикула
        result[['Цена', 'Скидка']] = result.groupby(
            'Артикул WB')[['Цена', 'Скидка']].ffill()
        # заполняем пустоты нужными значениями
        result[num_col] = result[num_col].fillna(0)
        result[string_cols] = result[string_cols].fillna('-')

        # сохраняем только те строки, которые есть в таблице stocks остатки
        right_only_rows = result[result['_merge'] == 'right_only']

        # в осноном дф удаляем строки которые есть только в правой таблице, они косячные
        result = result[result['_merge'] != 'right_only']

        # удаляем столбец _merge
        result = result.drop(columns='_merge')

        # группируем по столбцу итого остатки
        result = result.groupby([
            col for col in result.columns if col != 'Итого остатки'
        ])['Итого остатки'].sum().reset_index()

        # Создаем новый столбец Цена до СПП
        result['Цена до СПП'] = result['Цена'] * \
            (1 - result['Скидка']/100)

        # дф с артикулом и баркодом

        seller_article = result.filter([
            'Артикул WB', 'Баркод', 'Артикул поставщика', 'Размер'
        ])

        result = result.drop(columns=[
            'Баркод', 'Размер'
        ])

        result[['Цена', 'Скидка', 'Цена до СПП']] = result.groupby(
            'Артикул WB')[['Цена', 'Скидка', 'Цена до СПП']].transform('first')

        # группировка после удаления по сумме остатков
        result = result.groupby([
            col for col in result.columns if col != 'Итого остатки'
        ])['Итого остатки'].sum().reset_index()

        new_order = [
            'Артикул WB', 'ID KT', 'Артикул поставщика', 'Бренд', 'Наименование', 'Категория',
            'Итого остатки', 'Цена', 'Скидка', 'Цена до СПП', 'Фото', 'Ширина', 'Высота', 'Длина'
        ]

        # применяем новое расположение
        result = result[new_order]

        result = result.sort_values('Итого остатки', ascending=False)
        result['Кабинет'] = name

        if len(right_only_rows) > 0:
            logger.warning(
                f"косячная карточка кабинета {name} =  {right_only_rows.shape}\n{right_only_rows['Артикул WB'].to_list()}")

        else:
            logger.info(f"✅ Косячных карточек в {name} не найдено")

        logger.warning(
            f"🟢 Есть ли дубликаты в {name}?: {result.duplicated().any()}\n"
            f"📦 Колонки barcode_nmid: {seller_article.columns.tolist()}"
        )

    except Exception as e:
        msg = f"❌ Ошибка в процессе обработки данных: {e}"
        send_tg_message(msg)
        logger.error(msg)

    send_tg_message(
        f"✅ merge_and_transform_stocks_with_idkt завершена для {name}, строк: {len(result)}")

    return result, seller_article

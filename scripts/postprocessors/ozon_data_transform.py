from scripts.utils.setup_logger import make_logger
from scripts.utils.telegram_logger import send_tg_message
import pandas as pd

logger = make_logger(__name__, use_telegram=False)


def prepare_final_ozon_data(df_info: pd.DataFrame, df_stocks: pd.DataFrame, name: str) -> pd.DataFrame:
    """
    📦 Финальная подготовка данных для OZON: объединение карточек и остатков.

    Функция объединяет справочную информацию о товарах (`df_info`) с актуальными остатками (`df_stocks`),
    агрегирует данные, переименовывает колонки и подготавливает финальный DataFrame для выгрузки
    в формате, пригодном для OZON или внутреннего мониторинга.

    ─────────────────────────────────────────────────────────────

    🔧 Параметры:
    -------------
    df_info : pd.DataFrame
        Таблица карточек товаров OZON. Должна содержать колонку `sku` и другие атрибуты карточек.

    df_stocks : pd.DataFrame
        Таблица остатков товаров по SKU. Должна содержать поля:
        - `sku`
        - `name`
        - `available_stock_count`
        - `valid_stock_count`

    name : str
        Название кабинета или партнёра (используется в логах и уведомлениях Telegram).

    📤 Возвращает:
    --------------
    pd.DataFrame
        Объединённый и отфильтрованный DataFrame, содержащий:
        - базовые атрибуты карточек (артикул, упаковка, бренд, фото, цвет и т.д.);
        - количество доступного и годного товара;
        - тип карточки (`Тип`);
        - сортировку по доступному остатку по убыванию.

    📑 Описание обработки:
    ----------------------
    1. Агрегация остатков по SKU и названию (суммируются значения).
    2. Объединение карточек с остатками по `sku`.
    3. Обработка дубликатов колонок.
    4. Переименование колонок под финальный шаблон.
    5. Сортировка по убыванию остатков.
    6. Логирование каждого этапа и уведомления в Telegram при ошибках.

    ⚠️ Ошибки:
    ----------
    - При возникновении ошибки в любом блоке отправляется сообщение через `send_tg_message`.
    - В логах указывается источник ошибки и её описание.

    🧠 Автор: Илья  
    🗓 Версия: Июль 2025
    """
    send_tg_message(f"🚀 Запуск обработки OZON остатков для кабинета: {name}")
    try:
        logger.info(f"{name} - 📦 Фильтрация и агрегация остатков...")
        columns_to_keep = [
            'sku', 'name', 'available_stock_count', 'valid_stock_count',

        ]

        df_filtered_stocks = df_stocks[columns_to_keep]

        df_filtered_stocks = df_filtered_stocks.groupby([
            'sku', 'name',
        ]).agg({
            'available_stock_count': 'sum',
            'valid_stock_count': 'sum'
        }).reset_index()

        logger.info(
            f"{name} - ✅ Остатки сгруппированы: {len(df_filtered_stocks)} записей")
    except Exception:
        msg = f"{name} - ❌ Ошибка при фильтрации и агрегации остатков"
        logger.exception(msg)
        send_tg_message(msg)

    try:
        logger.info(f"{name} - 🔗 Объединяю данные карточек и остатков...")
        group_df = pd.merge(
            left=df_info,
            right=df_filtered_stocks,
            left_on='sku',
            right_on='sku',
            how='left',
            suffixes=('', '_stock'),
            indicator=True

        )
        logger.info(
            f"{name} - ✅ Объединение завершено. Статистика: \n{group_df['_merge'].value_counts().to_dict()}")

        if group_df.columns.duplicated().any():
            logger.warning(f"{name} - ⚠️ Повторяющиеся колонки:",
                           group_df.columns[group_df.columns.duplicated()])

            group_df = group_df.loc[:, ~group_df.columns.duplicated()]

        logger.info(f"{name} - ✏️ Переименованы колонки")

        group_df = group_df.rename(columns={

            'name': 'смени название',
            'available_stock_count': 'Количество единиц товара, доступное к продаже',
            'valid_stock_count': 'Количество товара без брака и с достаточным сроком годности',
        })

        group_df = group_df.filter([
            'Артикул', 'Название товара', 'Штрихкод', 'Ширина упаковки, мм', 'Высота упаковки, мм', 'Длина упаковки, мм',
            'Ссылка на главное фото', 'Бренд в одежде и обуви', 'Объединить на одной карточке', 'Цвет товара',
            'Количество единиц товара, доступное к продаже', 'Количество товара без брака и с достаточным сроком годности',
            'смени название'
        ])

        logger.info(f"{name} - 🧼 Отфильтрованы и переименованы нужные колонки")

        group_df = group_df.rename(
            columns={'смени название': 'Тип'}
        )

        group_df = group_df.sort_values(
            'Количество единиц товара, доступное к продаже', ascending=False)

        logger.info(f"{name} - 🔢 Отсортировано по остатку товара")

        return group_df
    except Exception:
        msg = f"{name} - ❌ Ошибка при обработке объединённого DataFrame"
        logger.exception(msg)
        send_tg_message(msg)

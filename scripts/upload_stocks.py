from scripts.gspread_client import get_gspread_client
from collections import defaultdict
import gspread
from gspread.exceptions import WorksheetNotFound, APIError
import pandas as pd
from scripts.setup_logger import make_logger
from scripts.telegram_logger import TelegramHandler

logger = make_logger(__name__, use_telegram=True)


def save_in_google_sheet(dict_data):
    """Выгружает данные из dict_data в Google Sheets по нескольким листам."""
    # сервис аккаунт гугл
    gc = get_gspread_client()
    # открываем гугл таблицу
    spreadsheet = gc.open('Ассортиментная матрица. Полная')

    def get_block_nmid():
        """Считывает из листа 'БЛОК' набор заблокированных NMID."""
        try:
            worksheet_block = None

            try:
                logger.info('🟢 получаем доступ к worksheet_block')

                worksheet_block = spreadsheet.worksheet('БЛОК')

            except WorksheetNotFound as e:
                logger.error(f"❌❌ [ОШИБКА] Лист БЛОК не найден: {e}")

            except APIError as e:
                logger.error(
                    f"❌❌ [ОШИБКА] Проблема с доступом к листу БЛОК: {e}")

            except Exception as e:
                logger.error(
                    f"❌❌ [НЕПРЕДВИДЕННАЯ ОШИБКА] при работе с листом БЛОК: {e}")

            # Если worksheet_block == True, то есть != "", None or 0
            if worksheet_block:
                try:
                    logger.info(
                        "🟢 Получаем список заблокированных nmid из листа 'БЛОК'")

                    block = set([
                        int(row[1])
                        for row in worksheet_block.get_all_values()[1:]
                        if row[0].strip().isdigit() and int(row[0]) == 0
                    ])

                    logger.info(
                        f"🟢 Получено {len(block)} заблокированных nmid ✅")
                    return block

                except Exception as e:
                    logger.error(
                        f"❌❌ Не удалось прочитать данные из листа 'БЛОК': {e}")
                    return set()

            else:
                logger.error(
                    "⚠️ Ошибка: лист 'БЛОК' не доступен — данные не загружены")
                return set()

        except Exception as e:
            logger.error(f"при получении block_nmid: {e} ❌❌")
            return set()

    # Создание рабочего листа

    def loading_all_cabinets(data, block):
        """Объединяет все df_tuple[0], фильтрует и пишет в лист 'API'.

        df_tuple[0] - основной дата фрейм с остатками и idkt

        """

        all_cabinet = pd.DataFrame()

        try:
            logger.info("🟢 Получаем доступ к листу 'API'")
            worksheet_idkt = spreadsheet.worksheet('API')

        except (WorksheetNotFound, APIError) as e:
            logger.error(
                f"❌{type(e).__name__} при доступе к листу 'API WB barcode' в таблице {worksheet_idkt}: {e}"
            )
        except Exception as e:
            logger.error(f"❌❌ при работе с листом API: {e}")
            return

        if worksheet_idkt:
            try:
                logger.info(
                    '🟢 Объединяем все кабинеты (df[0]) и фильтруем по условию')

                all_cabinet = pd.concat([
                    df_tuple[0]
                    for df_tuple in data.values()
                ], ignore_index=True)

                all_cabinet = all_cabinet[~all_cabinet['Артикул WB'].isin(
                    block)]

                if all_cabinet.empty:
                    logger.warning(
                        "❌❌⚠️ DataFrame all_cabinet пуст — пропущена выгрузка.")
                else:
                    worksheet_idkt.clear()

                    worksheet_idkt.update(
                        [all_cabinet.columns.values.tolist()] +
                        all_cabinet.values.tolist()
                    )

                    logger.info(
                        '🟢 В all_cards в лист API 🚀 — загрузка прошла быстро и мощно ✅'
                    )

            except Exception as e:
                logger.error(
                    f"❌❌⚠️ Ошибка при формировании или выгрузке all_cabinet в лист API: {e}"
                )
        else:
            logger.warning(
                "❌❌⚠️ Пропущена выгрузка all_cabinet: лист 'API' не доступен")

    def load_all_barcode(data_barcode):
        """Объединяет все df_tuple[1] и пишет в лист 'API 2'.

        df_tuple[1] - это дф со всеми баркодами всех кабинетов
        """

        try:
            worksheet_barcode = spreadsheet.worksheet('API 2')

        except (WorksheetNotFound, APIError) as e:
            logger.error(
                f"❌ {type(e).__name__} при доступе к листу 'API WB barcode' в таблице {worksheet_barcode}: {e}")
        except Exception as e:
            logger.error(
                f"❌❌⚠️ [НЕПРЕДВИДЕННАЯ ОШИБКА] при работе с листом 'API 2': {e}")

        if worksheet_barcode:
            logger.info('🟢 Объединяем все баркода (df_tuple[1]) из dick_data"')
            # выгружаем и объединяем все баркода
            barcode = pd.concat([
                df_tuple[1] for df_tuple in data_barcode.values()
            ], ignore_index=True)

            logger.info(f" 🟢 Всего баркодов {len(barcode)} 📤")

            worksheet_barcode.clear()

            worksheet_barcode.update([
                barcode.columns.values.tolist()] + barcode.values.tolist()
            )

            logger.info(
                f"📤 Загружено в лист 'API 2': {barcode.shape[0]} строк")

        else:
            logger.error(
                "❌❌⚠️ Пропущена выгрузка barcode: лист 'API 2' не доступен")

    def group_by_sheet(data):
        """Группирует barcode по заранее заданным группам."""

        GROUP_MAP = {
            'Фин модель Иосифовы Р А М': ['Азарья', 'Рахель', 'Михаил'],
            'Фин модель Галилова': ['Галилова'],
            'Фин модель Мартыненко': ['Мартыненко', 'Торгмаксимум']
        }
        grouped_df = defaultdict(pd.DataFrame)

        for name, (_, bcode) in data.items():
            for sheet, people in GROUP_MAP.items():
                if name in people:
                    grouped_df[sheet] = pd.concat(
                        [grouped_df[sheet], bcode], ignore_index=True)

        logger.info("📊 Данные сгруппированы! Обработка завершена 🛠️ 😎😎")
        return grouped_df

    def update_sheet(b_data):
        """Для каждой группы открывает свою таблицу и пишет barcode."""

        # загрузка баркодов по кабинетам
        for sheets, df in b_data.items():
            try:
                logger.info(f"🟢 Открываем таблицу {sheets}")
                sh = gc.open(sheets)
                wks = None
                try:
                    wks = sh.worksheet('API WB barcode')
                except (WorksheetNotFound, APIError) as e:
                    logger.error(
                        f"❌ {type(e).__name__} при доступе к листу 'API WB barcode' в таблице {sheets}: {e}")
                except Exception as e:
                    logger.error(
                        f"❌❌⚠️ при работе с листом 'API WB barcode' в таблице {sheets}: {e}")

                if wks:
                    try:
                        if df.empty:
                            logger.warning(
                                f"❌❌⚠️ Внимание: DataFrame для '{sheets}' пуст — пропускаем выгрузку.")
                            continue

                        wks.clear()
                        wks.update([df.columns.values.tolist()] +
                                   df.values.tolist())

                        logger.info(
                            f'🟢 Баркод загружен в таблицу {sheets}\nДлина: {df.shape} 🫡')

                    except Exception as e:
                        logger.error(
                            f" ❌❌⚠️ Ошибка при выгрузке данных в '{sheets}': {e}")
                else:
                    logger.error(
                        f"❌❌⚠️ Данные не выгружены в таблицу {sheets}: лист недотупен")

            except Exception as e:
                logger.error('❌❌⚠️',
                             f"в таблице '{sheets}': {e}\n данные не выгружены", sep='\n')

    block = get_block_nmid()
    loading_all_cabinets(data=dict_data, block=block)
    load_all_barcode(data_barcode=dict_data)
    grouped_data = group_by_sheet(data=dict_data)
    update_sheet(grouped_data)

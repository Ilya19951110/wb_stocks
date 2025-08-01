# wb_stocks
# test

# 📦 WB & OZON ETL Automation System

Система автоматической выгрузки, обработки и синхронизации данных с маркетплейсов **Wildberries** и **OZON**. Поддерживает асинхронную интеграцию с API, очистку и трансформацию данных, выгрузку в **Google Sheets**, а также уведомления через **Telegram** и запуск по расписанию через **GitHub Actions**.

---

## 🔧 Основной функционал

- 📤 **Выгрузка остатков, карточек и аналитики** из Wildberries и Ozon API
- 🧠 Обработка и агрегация данных:
  - Остатки
  - Воронка продаж
  - Рекламные кампании
  - SKU, barcode и ID-карточки
- 📊 **Интеграция с Google Sheets**:
  - Автоматическая выгрузка в заранее настроенные таблицы
  - Очистка диапазонов и создание листов при необходимости
- 🤖 **Асинхронная архитектура** (`aiohttp`, `asyncio`)
- 📬 **Уведомления в Telegram**:
  - Старт, успех, ошибки выполнения
- 🕐 **Запуск по расписанию** через GitHub Actions (каждый день в 6:00 МСК)

---

## 📁 Структура проекта


```plaintext
Iosifovy/
├── .github/
│   └── workflows/
│       ├── mywarehouse_api.yml
│       └── salles-funnel.yml
│
├── cache/                 # временные .pkl-файлы (лучше исключить)
│
├── scripts/
│   ├── engine/            # общий универсальный запуск
│   │   ├── run_cabinet.py
│   │   └── universal_main.py
│   │
│   ├── integrations/      # интеграции: WB, OZ, warehouse и пр.
│   │   ├── directory_wb_and_oz.py
│   │   ├── price_transfer_in_oz.py
│   │   └── split_and_upload_myWarehouse_sheets.py
│   │
│   ├── pipelines/         # получение и обработка данных
│   │   ├── get_advertising_report.py
│   │   ├── get_cards_list.py
│   │   ├── get_supplier_stocks.py
│   │   └── my_werehouse/
│   │       └── get_warehouse_api.py
│   │
│   ├── spreadsheet_tools/ # работа с Google Sheets
│   │   ├── upload_to_gsheet_advert_sales.py
│   │   └── update_barcode_by_tables.py
│   │
│   ├── utils/             # вспомогательные модули
│   │   ├── gspread_client.py
│   │   ├── telegram_logger.py
│   │   ├── setup_logger.py
│   │   └── config/
│   │       └── factory.py
│   │
│   └── tests/             # юнит-тесты
│       ├── test_run.py
│       ├── test_block_nmId_real.py
│       └── tests_get_advert.py
│
└── requirements.txt

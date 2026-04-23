# Wildberries Analytics App

Приложение на Streamlit с двумя вкладками:
- `Рекламная статистика` (WB Advertising API)
- `Позиции в поиске` (MPSTATS + WB Analytics fallback)

## Локальный запуск

```bash
python -m pip install -r requirements.txt
streamlit run app.py
```

## Основные переменные `.env`

- `WB_TOKEN`
- `LOG_LEVEL`
- `GOOGLE_CREDENTIALS_FILE`
- `GOOGLE_SPREADSHEET_ID`
- `MPSTATS_API_TOKEN`
- `WB_ANALYTICS_TOKEN` (опционально)
- `WB_CONTENT_TOKEN` (optional, preferred token for Content API)
- `WB_API_TOKEN` (optional alias for `WB_CONTENT_TOKEN`)

Полный список доступен в `.env.example`.

## Вкладка «Позиции в поиске»

UI не делает долгие API-запросы напрямую. Кнопка `Запустить проверку` ставит trigger в Google Sheets.
Сбор выполняет отдельный скрипт:

```bash
python run_collector.py --force --max-pairs 10
```

Для прод-режима используйте cron / GitHub Actions и запуск без `--force`.

## Логика источников (Dual Source)

Для каждой пары `nm_id + user_query`:
- `is_own_brand = true`:
  - сначала WB Analytics
  - fallback в MPSTATS при ошибке источника
  - fallback при `not_found` управляется `WB_FALLBACK_ON_NOT_FOUND`
- `is_own_brand = false`:
  - сразу MPSTATS

## Google Sheets: структура листов

### Settings (`Настройки`)
Минимум:
- `nm_id`
- `query` (или `user_query`)

Опционально:
- `product_name`
- `is_own_brand`

### Positions_Raw
Схема хранения:
- `date`
- `collected_at`
- `nm_id`
- `product_name`
- `user_query`
- `matched_query`
- `match_type`
- `position`
- `traffic_volume`
- `status` (`found` / `not_found` / `source_error`)
- `data_source` (`wb_analytics` / `mpstats`)
- `error_msg`

Ключ upsert: `(date, nm_id, user_query)`.

### Positions_State
Служебные ключи статуса коллектора:
- `trigger_pending`
- `trigger_requested_at`
- `running`
- `last_run_status`
- `last_run_time`
- `last_run_rows`
- `last_error`

## Планировщик

В репозитории есть workflow: `.github/workflows/positions_collector.yml`.
Он периодически запускает `run_collector.py` и обрабатывает pending-trigger.

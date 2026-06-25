# Disputes Reconciliation — Контекст

Сверка диспутов (PayPal, Stripe, Adyen) с тикетами Zendesk. Расследование мая 2026.

## Задача

Сматчить записи диспутов из платёжных систем с тикетами Zendesk по email, датам и суммам.
Проверить покрытие: какие диспуты имеют тикет, какие — нет.

## Структура

```
sql/
  _tmp_col_exists_prev_status.sql   — проверка колонок Stripe
  _tmp_show_cols_stripe_disputes.sql
  _tmp_show_tables_stripe.sql
  _tmp_stripe_email_columns.sql
  _tmp_stripe_fraud_key_columns.sql

data/
  Adyen_EU/UAE/US_dispute_report_2025-10-01_2025-12-31.csv  — выгрузки Adyen (oct–dec 2025)
  january 2026/                     — выгрузки за январь
  february 2026/                    — выгрузки за февраль
  paypal - Apextech.CSV             — PayPal диспуты (Apextech entity)
  paypal - Stellartech.CSV          — PayPal диспуты (Stellartech entity)
  stripe_25032026 - US/gateway.csv  — Stripe диспуты (март 2026)
  disputes_jan_feb_dedup_usd.csv    — дедуп январь-февраль

output/
  all_emails_disputes_fraud_match_summary.csv
  all_emails_matched_disputes.csv
  all_emails_matched_fraud_warnings.csv
  disputes_merged_getaway_us.csv
  disputes_tickets_email_matches.csv
  disputes_without_ticket_match.csv
  fraud_warnings_enriched_with_utc_by_charge_id.csv
  paypal_3_disputes_*.json          — детали 3 PayPal-диспутов из API
  tickets_disputes_*.csv            — результаты матчинга тикетов и диспутов
  tickets_match_3_sources_summary.csv
  tickets_without_dispute_match.csv

disputes_data_model_open_sources_2026-05-04.md    — модель данных диспутов
paypal_disputes_3cases_reconciliation_2026-05-07.md — разбор 3 PayPal-кейсов
stripe_disputes_reconciliation_report_2026-05-07.md — итоговый отчёт по Stripe
```

## Источники данных

- Adyen, Stripe, PayPal — экспорты из личных кабинетов (в `data/`)
- `data_silver_zendesk_prod.zendesk_events` — тикеты Zendesk
- Stripe таблицы в Athena (через `_tmp_show_tables_stripe.sql`)

## Статус

Завершён. Итоговые отчёты: `stripe_disputes_reconciliation_report_2026-05-07.md` и `paypal_disputes_3cases_reconciliation_2026-05-07.md`.

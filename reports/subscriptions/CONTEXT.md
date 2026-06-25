# Subscriptions — Контекст

Аналитика платных подписок: отмены и upsell-метрики.

## Файлы

| Файл | Назначение |
|------|-----------|
| `[actual] charged canceled subs.sql` | Подписки которые были списаны и затем отменены. Источники: Payrails, Stripe, Adapty. Старт с 2025-01-01. |
| `[actual] main_subs_and_upsell.sql` | Основные подписки + upsell. Источник: Chargebee / product sessions. |

## Источники данных

- `firehose_payrails_webhook_prod.payrails` — Payrails транзакции
- `data_silver_product_sessions_prod.sf_purchase_sessions` — purchase sessions
- `chargebee_product_catalog_2.subscription` — Chargebee подписки
- Stripe charge/transaction tables
- `data_bronze_supabase_prod.smartyme_public__subscriptions` — Adapty подписки

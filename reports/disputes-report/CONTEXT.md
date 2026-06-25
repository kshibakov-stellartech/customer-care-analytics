# Disputes Report — Контекст

Отчёт по диспутам: сводная таблица и детальные расследования по конкретным кейсам.

## Файлы

| Файл | Назначение |
|------|-----------|
| `dispute_data.sql` | Сводный датасет: все диспуты из Adyen + Stripe + PayPal в единой схеме. |
| `dispute_inv_table [final].sql` | Детальная таблица расследования по конкретным dispute_id (хардкодятся в `dispute_list` CTE). Финальная версия. |
| `dispute_inv_table [stripe].sql` | Stripe-версия детальной таблицы расследования. |
| `dispute_investigation_table.sql` | Общая таблица расследования (исходная версия). |

## Источники данных

- `data_bronze_stripe_prod.stripe_charge_dispute_created` — Stripe диспуты
- Adyen dispute tables — Adyen диспуты
- PayPal dispute tables — PayPal диспуты

## Как использовать

Для нового расследования конкретных диспутов: открой `dispute_inv_table [final].sql`,
замени значения в CTE `dispute_list` на нужные `dispute_id`.

Для общей статистики по всем диспутам: `dispute_data.sql`.

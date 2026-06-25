# Stripe Disputes Project — Контекст

Исследование: как замэтчить email пользователей на диспуты в Stripe DWH.

## Задача

Нужно было связать список из ~100 email-адресов с диспутами в Stripe.
Проблема: в таблицах Stripe DWH нет явного поля с email — нужно найти правильный ключ.

## Структура sql/

```
01_stripe_disputes_with_user_email.sql    — финальный запрос: диспуты с email пользователя
02_stripe_disputes_with_customer_email.sql — вариант с customer email (billing details)

_tmp_stripe_user_identifier_columns.sql       — поиск колонок-идентификаторов
_tmp_stripe_user_identifier_columns_clean.sql — чистая версия поиска
_tmp_stripe_user_identifier_columns_strict.sql — строгий фильтр
_tmp_find_customer_key_columns.sql            — поиск customer_key
_tmp_customer_key_reality_checks.sql          — проверка customer_key

_tmp_match_100_emails_to_disputes.sql               — матчинг 100 emails → диспуты
_tmp_match_100_emails_to_disputes_counts.sql        — статистика матчинга
_tmp_match_100_emails_to_disputes_missing_sample.sql — примеры незамэтченных
_tmp_match_100_emails_to_disputes_customer_*.sql    — через customer_email
_tmp_match_100_emails_to_dispute_updated_*.sql      — обновлённая версия

_tmp_input_100_emails.csv — входной список email-адресов
```

## Источники данных

- Stripe DWH таблицы в Athena (точные названия — в запросах 01/02)
- Email берётся из: `data__object__evidence__customer_email_address`, `billing_details__email`, `metadata__customer_email` — COALESCE по убыванию надёжности

## Статус

Завершён. Финальные запросы: `01_stripe_disputes_with_user_email.sql` и `02_stripe_disputes_with_customer_email.sql`.
Результаты расследования трёх кейсов — в `sandbox/disputes/`.

## Как пользоваться с Cowork

Для нового матчинга email → диспуты: возьми запрос `01_stripe_disputes_with_user_email.sql` как основу.
Для проверки покрытия: `_tmp_match_100_emails_to_disputes_counts.sql`.

# Trustpilot Funnel — Контекст

Воронка Trustpilot: от тикета до отзыва на Trustpilot.

## Файлы

| Файл | Назначение |
|------|-----------|
| `tp_funnel_data.sql` | Данные воронки: тикеты → приглашения → отзывы на Trustpilot. Использует те же excluded tags что и cc-report. |

## Источники данных

- `data_bronze_zendesk_prod.zendesk_audit`
- `data_silver_zendesk_prod.zendesk_events`
- Trustpilot review tables

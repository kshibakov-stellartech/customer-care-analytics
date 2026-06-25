# CSAT Validation — Контекст

Валидация качества и консистентности CSAT-данных. Апрель–май 2026.

## Что исследовали

Расхождение между двумя версиями CSAT-логики (v1 vs v2) и соответствие данных
из трёх источников: Zendesk tickets, Appfollow reviews, Trustpilot reviews.

## Структура

```
sql/
  csat_q1..q11_*.sql   — ключевые вопросы CSAT-валидации (q1..q8 = основные, q10–q11 = итог)
  _tmp_*.sql           — вспомогательные запросы (appfollow quality checks, trustpilot, zendesk)

scripts/
  run_athena_query.sh  — запуск запросов в Athena
```

## Ключевые запросы

| Запрос | Что делает |
|--------|-----------|
| `csat_q1_event_distribution.sql` | Распределение CSAT-событий |
| `csat_q2_matching_quality.sql` | Качество матчинга Zendesk ↔ отзывы |
| `csat_q3_coverage.sql` | Покрытие CSAT данных |
| `csat_q8_v1_vs_v2_comparison.sql` | Сравнение двух версий логики CSAT |
| `csat_q10_ticket_level_match_stats.sql` | Статистика матчинга на уровне тикета |
| `csat_q11_actual_vs_draft_summary.sql` | Итоговое сравнение actual vs draft |

## Источники данных

- `data_silver_zendesk_prod.zendesk_events` / `data_bronze_zendesk_prod.zendesk_audit`
- Appfollow reviews history
- Trustpilot reviews history

## Статус

Завершён. Asana-запросы перенесены в `adhocs/asana-task-attrs/`. Диспутные запросы и output — в `adhocs/disputes-reconciliation/`.

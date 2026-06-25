# VOC Adoption — Контекст

Метрики adoption VOC-тегов (Voice of Customer): насколько агенты проставляют теги.

## Файлы

| Файл | Назначение |
|------|-----------|
| `voc_adoption.sql` | Абсолютные значения adoption: сколько тикетов получили VOC-тег. |
| `voc_adoption_rate.sql` | Adoption rate: доля тикетов с VOC-тегом от общего числа. |

## Источники данных

- `data_silver_zendesk_prod.zendesk_events` — события тикетов
- `data_bronze_zendesk_prod.zendesk_audit` — аудит

## Примечание

Оба запроса используют те же excluded tags и bot ID что и cc-report.
Bot ID (Nikki): `26440502459665`

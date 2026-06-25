# CC Report — Контекст

Основной Customer Care дашборд в Tableau. Метрики поддержки SmartyMe: тикеты, каналы, агенты, CSAT, VOC.

## Файлы

| Файл | Назначение |
|------|-----------|
| `[actual] cc report - overall.sql` | Главный запрос: тикеты по каналам, статусам, CSAT, VOC-теги. Продакшн. |
| `cc report by agents view.sql` | То же, но с разбивкой по агентам: объём, FRT, SRT. |
| `[actual] voc_tags_logic.sql` | Логика определения VOC-тегов на тикете. Используется в обоих запросах выше. |
| `[draft] cc report - overall - csat-multiscore.sql` | Черновик с мультиоценочным CSAT. **Не в проде.** |

## Источники данных

- `data_bronze_zendesk_prod.zendesk_audit` — raw аудит тикетов
- `data_silver_zendesk_prod.zendesk_events` — обработанные события

## Исключения (excluded tags)

`cancellation_notification`, `closed_by_merge`, `voice_abandoned_in_voicemail`, `appfollow`, `spam`, `ai_cb_triggered`, `chargeback_precom`, `chargeback_postcom`

Bot ID (Nikki): `26440502459665`

## Как обновлять

Основные изменения — в `[actual] cc report - overall.sql` и `cc report by agents view.sql`.
Если меняется логика VOC — обновить `voc_tags_logic.sql` и синхронизировать в оба отчёта.

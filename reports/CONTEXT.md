# reports/ — Контекст

Регулярные отчёты и дашборды. Каждая папка = отдельный отчёт со своим SQL и `CONTEXT.md`.

## Проекты

| Папка | Что делает | Tableau |
|-------|-----------|---------|
| `cc-report/` | Основной CC-дашборд: тикеты, агенты, каналы, CSAT, VOC-теги | ✅ |
| `disputes-report/` | Таблица расследований диспутов (финальная + Stripe-версия) | ✅ |
| `duplicates/` | Еженедельный отчёт: дубли подписчиков. CSV-данные в `data/` | — |
| `user-scopes/` | Еженедельный отчёт: скоупы пользователей. CSV-данные в `data/` | — |
| `subscriptions/` | Платные подписки с отменой + main upsell метрики | ✅ |
| `voc-adoption/` | VOC adoption + adoption rate | ✅ |
| `community/` | Отзывы Trustpilot + Appfollow в единой таблице | ✅ |
| `tp-funnel/` | Trustpilot-воронка: тикет → приглашение → отзыв | ✅ |
| `workload/` | Моделирование нагрузки на агентов | — |

## Общие источники данных

- `data_silver_zendesk_prod.zendesk_events` — основной источник тикетов
- `data_bronze_zendesk_prod.zendesk_audit` — raw Zendesk аудит
- Bot ID (Nikki): `26440502459665`

## Как работать с Cowork

Открой папку конкретного отчёта → прочти `CONTEXT.md` → запускай SQL в Athena.

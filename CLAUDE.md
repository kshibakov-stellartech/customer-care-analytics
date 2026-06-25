# customer-care-analytics — общий контекст

Личное рабочее пространство для аналитики поддержки SmartyMe.

## Структура репозитория

```
reports/    — регулярные отчёты и дашборды (каждый SQL-файл = отдельный проект)
adhocs/     — расследования и ad-hoc анализы (каждая папка = самостоятельный проект)
projects/   — инструменты и долгосрочные проекты
infra/      — инфраструктурные скрипты: обновление AWS-кредов, запуск Athena-запросов и т.п.
archive/    — всё старое: черновики, устаревшие данные, старые версии
```

Каждый проект содержит `CONTEXT.md` с деталями.

---

## reports/

| Папка | Что делает | Статус |
|-------|-----------|--------|
| `cc-report/` | Основной CC-дашборд Tableau: тикеты, агенты, каналы, CSAT, VOC-теги | 🟢 активный |
| `disputes-report/` | Таблица расследований диспутов (финальная + Stripe-версия) | 🟢 активный |
| `duplicates/` | Еженедельный отчёт: дубли подписчиков. Данные в `data/` | 🟢 активный |
| `user-scopes/` | Еженедельный отчёт: скоупы пользователей. Данные в `data/` | 🟢 активный |
| `subscriptions/` | Платные подписки с отменой + main upsell метрики | 🟢 активный |
| `voc-adoption/` | VOC adoption + adoption rate | 🟢 активный |
| `community/` | Community-метрики | 🟢 активный |
| `tp-funnel/` | Trustpilot-воронка | 🟢 активный |
| `workload/` | Моделирование нагрузки на агентов | 🟢 активный |

---

## adhocs/

| Папка | Что исследовалось | Статус |
|-------|------------------|--------|
| `csat-validation-2026-04/` | Валидация CSAT-данных: Zendesk vs Appfollow vs Trustpilot | ✅ завершён |
| `stripe-disputes-2026/` | Матчинг email → Stripe-диспуты в DWH | ✅ завершён |
| `asana-task-attrs/` | Атрибуты и периоды обработки задач Asana (project 1211305108470489) | ✅ завершён |
| `disputes-reconciliation/` | Сверка диспутов (PayPal, Stripe, Adyen) с тикетами Zendesk | ✅ завершён |
| `adapty-search/` | SQL: поиск пользователя по email в Adapty | 🛠️ утилита |

---

## projects/

| Папка | Что делает | Статус |
|-------|-----------|--------|
| `tableau-publisher/` | Python-скрипт публикации датасорсов в Tableau | 🟢 активный |
| `tech-support-tasks/` | SQL: датасет задач tech support из Asana | 🛠️ утилита |

---

## Ключевые источники данных

- `data_silver_zendesk_prod.zendesk_events` — основной источник тикетов
- `data_bronze_zendesk_prod.zendesk_audit` — raw Zendesk аудит
- `data_bronze_supabase_prod.smartyme_public__subscriptions` — подписки SmartyMe (Adapty)
- `fivetran_asana.*` — Asana данные
- Bot/automation agent ID: `26440502459665` (Nikki)

## Как работать с Cowork

- Открой папку конкретного проекта → прочти `CONTEXT.md`
- Для нового adhoc: создай папку `adhocs/<название>-<YYYY-MM>/` с `CONTEXT.md` внутри
- Для нового отчёта: создай папку `reports/<название>/` с SQL и `CONTEXT.md`
- После завершения проекта: обнови статус в этом файле

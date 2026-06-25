# projects/ — Контекст

Инструменты и долгосрочные проекты. Каждая папка = самостоятельный проект.

## Проекты

| Папка | Что делает | Статус |
|-------|-----------|--------|
| `tableau-publisher/` | Python-скрипт публикации датасорсов в Tableau Cloud | 🟢 активный |
| `tech-support-tasks/` | SQL: датасет задач tech support из Asana для Tableau | 🛠️ утилита |

## tableau-publisher/

Скрипт `scripts/publish_datasource.py` публикует `.tds`/`.tdsx` файлы в Tableau Cloud.
`scripts/sync_sqltools_athena_connection.py` — синхронизация Athena-коннекшена в SQLTools.
Конфигурация в `.env` (не в git).

## tech-support-tasks/

`tech_support_tasks_dataset.sql` — финальный SQL для датасета задач tech support из Asana.
Использует `fivetran_asana.*`. Публикуется в Tableau через `tableau-publisher/`.

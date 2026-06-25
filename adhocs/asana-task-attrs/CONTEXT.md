# Asana Task Attributes Project — Контекст

Анализ атрибутов и жизненного цикла Asana-задач для project_id `1211305108470489`.

## Задача

Построить аналитический датасет по задачам Asana: 
- Какие кастомные поля заполняются и как меняются со временем
- Периоды обработки (processing periods) — сколько задача находится в каждом статусе
- Распределение issue_category и issue_user_type по месяцам

## Структура

```
sql/
  01_task_attrs.sql                 — снепшот: текущие атрибуты задач
  02_history_attrs.sql              — история изменений атрибутов
  03_task_attrs_wide.sql            — пивот атрибутов в широкий формат
  04_processing_periods.sql         — периоды обработки задачи
  05_task_processing_summary.sql    — сводка обработки (ФИНАЛЬНАЯ ТАБЛИЦА)
  06_story_*.sql                    — кастомные поля: issue_user_type
  07_issue_user_type_history*.sql   — история issue_user_type
  08_issue_user_type_monthly*.sql   — месячное распределение issue_user_type
  09_issue_category_*.sql           — распределение issue_category
  10_issue_category_history_monthly.sql — история issue_category по месяцам
  exploration/                      — разведочные запросы (probe, show_tables, column scans)

scripts/
  run_tech_support_probe.sh         — запуск probe-запросов в Athena
  tech_support_tasks_probe.sql      — probe-запрос для tech support задач

output/
  task_1212082488015300_wide.*      — экспорт конкретной задачи (CSV + JSON)
```

Файлы с суффиксами `_checks`, `_anomalies`, `_samples` — диагностические.
Файлы с `_1212082488015300` — примеры на конкретной задаче для отладки.

## Источники данных

- `fivetran_asana.project_task` — связь проект-задача
- `fivetran_asana.project` — названия проектов
- `fivetran_asana.task` — атрибуты задач
- `fivetran_asana.story` — история изменений (story events)
- Проект: `1211305108470489`

## Статус

Завершён. Основной результат: `05_task_processing_summary.sql`.

## Как пользоваться с Cowork

Запросы пронумерованы — выполняй последовательно для воспроизведения полного датасета.
Для нового проекта — поменяй `project_id` в `01_task_attrs.sql`.

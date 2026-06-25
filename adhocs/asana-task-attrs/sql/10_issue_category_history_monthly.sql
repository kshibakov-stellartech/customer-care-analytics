WITH base_tasks AS (
  SELECT DISTINCT task_id
  FROM fivetran_asana.project_task
  WHERE project_id = '1211305108470489'
),
issue_category_events AS (
  SELECT
    s.target_id AS task_id,
    s.created_at AS event_at
  FROM fivetran_asana.story s
  JOIN base_tasks bt
    ON bt.task_id = s.target_id
  WHERE trim(coalesce(s.text, '')) <> ''
    AND regexp_like(lower(s.text), 'changed issue category from .* to .*')
)
SELECT
  date_trunc('month', event_at) AS event_month,
  COUNT(*) AS events_cnt,
  COUNT(DISTINCT task_id) AS tasks_cnt
FROM issue_category_events
GROUP BY 1
ORDER BY 1

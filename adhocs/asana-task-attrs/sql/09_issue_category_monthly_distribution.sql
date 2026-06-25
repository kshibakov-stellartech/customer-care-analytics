WITH base_tasks AS (
  SELECT DISTINCT task_id
  FROM fivetran_asana.project_task
  WHERE project_id = '1211305108470489'
),
story_changes AS (
  SELECT
    s.target_id AS task_id,
    s.created_at AS event_at,
    regexp_extract(s.text, '(?i)changed (.*?) from ', 1) AS changed_field
  FROM fivetran_asana.story s
  JOIN base_tasks bt
    ON bt.task_id = s.target_id
  WHERE trim(coalesce(s.text, '')) <> ''
    AND regexp_like(lower(s.text), 'changed .* from .* to .*')
),
normalized AS (
  SELECT
    task_id,
    date_trunc('month', event_at) AS event_month,
    lower(trim(changed_field)) AS changed_field_norm
  FROM story_changes
)
SELECT
  event_month,
  changed_field_norm,
  COUNT(*) AS events_cnt,
  COUNT(DISTINCT task_id) AS tasks_cnt
FROM normalized
WHERE changed_field_norm IN ('issue category', 'issue type')
GROUP BY 1,2
ORDER BY 1,2

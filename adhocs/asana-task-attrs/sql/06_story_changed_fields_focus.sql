WITH base_tasks AS (
  SELECT DISTINCT task_id
  FROM fivetran_asana.project_task
  WHERE project_id = '1211305108470489'
), parsed AS (
  SELECT
    s.target_id AS task_id,
    s.created_at,
    s.text,
    lower(coalesce(s.text, '')) AS ltext,
    regexp_extract(s.text, '(?i)changed (.*?) from ', 1) AS changed_field
  FROM fivetran_asana.story s
  JOIN base_tasks bt ON bt.task_id = s.target_id
  WHERE trim(coalesce(s.text, '')) <> ''
    AND regexp_like(lower(s.text), 'changed .* from .* to .*')
)
SELECT
  coalesce(changed_field, 'NULL') AS changed_field,
  COUNT(*) AS events_cnt,
  COUNT(DISTINCT task_id) AS tasks_cnt
FROM parsed
WHERE regexp_like(lower(coalesce(changed_field, '')), 'issue type|user type|category|type')
GROUP BY 1
ORDER BY events_cnt DESC
LIMIT 50

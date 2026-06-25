WITH base_tasks AS (
  SELECT DISTINCT task_id
  FROM fivetran_asana.project_task
  WHERE project_id = '1211305108470489'
)
SELECT
  s.target_id AS task_id,
  s.created_at,
  s.text
FROM fivetran_asana.story s
JOIN base_tasks bt ON bt.task_id = s.target_id
WHERE regexp_like(lower(coalesce(s.text, '')), 'changed issue category from .* to .*')
ORDER BY s.created_at DESC
LIMIT 30

WITH base_tasks AS (
  SELECT DISTINCT task_id
  FROM fivetran_asana.project_task
  WHERE project_id = '1211305108470489'
), story_base AS (
  SELECT
    s.target_id AS task_id,
    s.created_at,
    s.text,
    lower(coalesce(s.text, '')) AS ltext
  FROM fivetran_asana.story s
  JOIN base_tasks bt ON bt.task_id = s.target_id
  WHERE trim(coalesce(s.text, '')) <> ''
)
SELECT
  task_id,
  created_at,
  text
FROM story_base
WHERE regexp_like(ltext, 'changed .* from .* to .*')
  AND (
    regexp_like(ltext, 'app issue|app access issue|subscription & payment issue|subscription upgrade request|smth went wrong screen|account linked to another profile|other app access issue|payment not captured|discount was not applied|cancelled but charged|paid but no access to sub|store sub is not linked to profile|other sub & payment issue|content type change')
    OR regexp_like(ltext, 'category')
    OR regexp_like(ltext, 'user type')
  )
ORDER BY created_at DESC
LIMIT 50

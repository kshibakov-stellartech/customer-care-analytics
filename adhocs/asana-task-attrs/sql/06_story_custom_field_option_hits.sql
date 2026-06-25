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
  COUNT(*) AS story_rows,
  SUM(CASE WHEN regexp_like(ltext, 'changed .* from .* to .*') THEN 1 ELSE 0 END) AS changed_from_to_rows,
  SUM(CASE WHEN regexp_like(ltext, 'changed .*category.*') THEN 1 ELSE 0 END) AS changed_category_rows,
  SUM(CASE WHEN regexp_like(ltext, 'app issue|app access issue|subscription & payment issue|subscription upgrade request') THEN 1 ELSE 0 END) AS top_option_rows,
  SUM(CASE WHEN regexp_like(ltext, 'smth went wrong screen|account linked to another profile|other app access issue|payment not captured|discount was not applied|cancelled but charged|paid but no access to sub|store sub is not linked to profile|other sub & payment issue|content type change') THEN 1 ELSE 0 END) AS sub_option_rows
FROM story_base

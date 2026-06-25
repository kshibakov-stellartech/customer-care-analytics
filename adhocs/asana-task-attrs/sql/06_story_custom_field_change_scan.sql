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
), classified AS (
  SELECT
    task_id,
    created_at,
    text,
    CASE
      WHEN regexp_like(ltext, 'changed .* from .* to .*') THEN 'changed_from_to'
      WHEN regexp_like(ltext, 'changed .* to .*') THEN 'changed_to'
      WHEN regexp_like(ltext, 'set .* to .*') THEN 'set_to'
      ELSE 'other'
    END AS change_type,
    CASE
      WHEN regexp_like(ltext, 'changed ([^,.;:]*) from ') THEN regexp_extract(text, '(?i)changed ([^,.;:]*) from ', 1)
      WHEN regexp_like(ltext, 'changed ([^,.;:]*) to ') THEN regexp_extract(text, '(?i)changed ([^,.;:]*) to ', 1)
      WHEN regexp_like(ltext, 'set ([^,.;:]*) to ') THEN regexp_extract(text, '(?i)set ([^,.;:]*) to ', 1)
      ELSE NULL
    END AS field_name_guess
  FROM story_base
), target_hits AS (
  SELECT
    *,
    CASE
      WHEN regexp_like(ltext, 'app issue|app access issue|subscription & payment issue|subscription upgrade request|other') THEN 1
      ELSE 0
    END AS top_category_option_hit,
    CASE
      WHEN regexp_like(ltext, 'smth went wrong screen|account linked to another profile|other app access issue|payment not captured|discount was not applied|cancelled but charged|paid but no access to sub|store sub is not linked to profile|other sub & payment issue|content type change') THEN 1
      ELSE 0
    END AS sub_category_option_hit
  FROM story_base
)
SELECT 'change_types' AS block, change_type AS key1, cast(count(*) as varchar) AS val1, '' AS val2
FROM classified
GROUP BY 1,2
UNION ALL
SELECT 'field_name_guess_top20' AS block, coalesce(field_name_guess, 'NULL') AS key1, cast(count(*) as varchar) AS val1, '' AS val2
FROM classified
WHERE field_name_guess IS NOT NULL
GROUP BY 1,2
ORDER BY val1 DESC
LIMIT 20

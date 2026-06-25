WITH base_tasks AS (
  SELECT DISTINCT task_id
  FROM fivetran_asana.project_task
  WHERE project_id = '1211305108470489'
)
SELECT
  regexp_extract(s.text, '^(.*?) changed ', 1) AS actor_name,
  COUNT(*) AS events_cnt
FROM fivetran_asana.story s
JOIN base_tasks bt ON bt.task_id = s.target_id
WHERE regexp_like(lower(coalesce(s.text, '')), 'changed (issue type|user type) from .* to .*')
GROUP BY 1
ORDER BY events_cnt DESC
LIMIT 20

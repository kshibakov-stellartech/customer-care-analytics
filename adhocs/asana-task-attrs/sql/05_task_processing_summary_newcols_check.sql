WITH res AS (
  SELECT *
  FROM (
    -- inline read from final table query not needed; reuse same source logic minimal
    WITH base_tasks AS (
      SELECT DISTINCT pt.task_id
      FROM fivetran_asana.project_task pt
      WHERE pt.project_id = '1211305108470489'
    ),
    story_changes AS (
      SELECT
        s.target_id AS task_id,
        s.created_at AS event_at,
        regexp_extract(s.text, '(?i)changed (.*?) from ', 1) AS changed_field,
        regexp_extract(s.text, '(?i) to (.*)$', 1) AS current_value
      FROM fivetran_asana.story s
      JOIN base_tasks bt ON bt.task_id = s.target_id
      WHERE trim(coalesce(s.text, '')) <> ''
        AND regexp_like(lower(s.text), 'changed .* from .* to .*')
    ),
    last_vals AS (
      SELECT
        task_id,
        max_by(current_value, event_at) FILTER (WHERE regexp_like(lower(changed_field), 'issue type')) AS last_issue_type,
        max_by(current_value, event_at) FILTER (WHERE regexp_like(lower(changed_field), 'issue category')) AS last_issue_category
      FROM story_changes
      GROUP BY task_id
    )
    SELECT bt.task_id, lv.last_issue_type, lv.last_issue_category
    FROM base_tasks bt
    LEFT JOIN last_vals lv ON lv.task_id = bt.task_id
  ) x
)
SELECT
  COUNT(*) AS rows_total,
  COUNT(DISTINCT task_id) AS tasks_distinct,
  COUNT(*) - COUNT(DISTINCT task_id) AS dup_rows,
  SUM(CASE WHEN last_issue_type IS NOT NULL THEN 1 ELSE 0 END) AS last_issue_type_filled,
  SUM(CASE WHEN last_issue_category IS NOT NULL THEN 1 ELSE 0 END) AS last_issue_category_filled
FROM res

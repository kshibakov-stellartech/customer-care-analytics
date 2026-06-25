-- 07_issue_user_type_history.sql
-- Normalized history for Issue type / User type from Asana story text

WITH base_tasks AS (
  SELECT DISTINCT task_id
  FROM fivetran_asana.project_task
  WHERE project_id = '1211305108470489'
),
story_changes AS (
  SELECT
    s.target_id AS task_id,
    s.id AS story_id,
    s.created_at AS event_at,
    s.text,
    regexp_extract(s.text, '(?i)changed (.*?) from ', 1) AS changed_field,
    regexp_extract(s.text, '(?i) from (.*?) to ', 1) AS previous_value,
    regexp_extract(s.text, '(?i) to (.*)$', 1) AS current_value
  FROM fivetran_asana.story s
  JOIN base_tasks bt
    ON bt.task_id = s.target_id
  WHERE trim(coalesce(s.text, '')) <> ''
    AND regexp_like(lower(s.text), 'changed .* from .* to .*')
),
normalized AS (
  SELECT
    task_id,
    story_id,
    event_at,
    changed_field,
    trim(previous_value) AS previous_value,
    trim(current_value) AS current_value,
    CASE
      WHEN lower(trim(changed_field)) = 'issue type' THEN 'issue_type'
      WHEN lower(trim(changed_field)) = 'user type' THEN 'user_type'
      ELSE NULL
    END AS attribute_name
  FROM story_changes
),
dedup AS (
  SELECT
    task_id,
    event_at,
    attribute_name,
    previous_value,
    current_value,
    row_number() OVER (
      PARTITION BY task_id, attribute_name, coalesce(previous_value, ''), coalesce(current_value, ''), date_trunc('second', event_at)
      ORDER BY story_id
    ) AS rn
  FROM normalized
  WHERE attribute_name IS NOT NULL
)
SELECT
  task_id,
  attribute_name,
  event_at,
  previous_value,
  current_value
FROM dedup
WHERE rn = 1
ORDER BY task_id, event_at, attribute_name
;

-- 02_history_attrs.sql
-- Historical attributes (multiple rows per task)

WITH base_tasks AS (
  SELECT DISTINCT
    pt.task_id
  FROM fivetran_asana.project_task pt
  WHERE pt.project_id = '1211305108470489'
),
history_raw AS (
  SELECT
    s.target_id AS task_id,
    s.id AS story_id,
    s.created_at AS event_at,
    s.text,
    CASE
      WHEN regexp_like(lower(s.text), 'moved this task from ".*" to ".*"')
        OR regexp_like(lower(s.text), 'changed section from .* to .*') THEN 'section'
      WHEN regexp_like(lower(s.text), 'changed task progress') THEN 'task_progress'
      WHEN regexp_like(lower(s.text), 'assigned to |unassigned|removed assignee') THEN 'assignee'
      WHEN regexp_like(lower(s.text), 'changed the due date|removed the due date') THEN 'due_date'
      WHEN regexp_like(lower(s.text), 'changed completed date') THEN 'completed_date'
      WHEN regexp_like(lower(s.text), 'marked this task complete|completed this task|marked this task incomplete') THEN 'completion_status'
      WHEN regexp_like(lower(s.text), 'changed .* to .*') THEN regexp_extract(s.text, '(?i)changed (.*) to .*', 1)
      ELSE NULL
    END AS changed_field,
    CASE
      WHEN regexp_like(lower(s.text), 'moved this task from ".*" to ".*"')
        THEN regexp_extract(s.text, 'from "([^"]*)" to "', 1)
      WHEN regexp_like(lower(s.text), 'changed section from .* to .*')
        THEN regexp_extract(s.text, '(?i)changed section from (.*) to ', 1)
      WHEN regexp_like(lower(s.text), 'changed task progress from .* to .*')
        THEN regexp_extract(s.text, '(?i)changed task progress from (.*) to ', 1)
      WHEN regexp_like(lower(s.text), 'removed assignee') THEN 'assigned'
      ELSE NULL
    END AS previous_value,
    CASE
      WHEN regexp_like(lower(s.text), 'moved this task from ".*" to ".*"')
        THEN regexp_extract(s.text, 'to "([^"]*)"', 1)
      WHEN regexp_like(lower(s.text), 'changed section from .* to .*')
        THEN regexp_extract(s.text, '(?i) to (.*)$', 1)
      WHEN regexp_like(lower(s.text), 'changed section to .*')
        THEN regexp_extract(s.text, '(?i)changed section to (.*)$', 1)
      WHEN regexp_like(lower(s.text), 'changed task progress from .* to .*')
        THEN regexp_extract(s.text, '(?i) to (.*)$', 1)
      WHEN regexp_like(lower(s.text), 'changed .* to .*')
        THEN regexp_extract(s.text, '(?i)changed .* to (.*)$', 1)
      WHEN regexp_like(lower(s.text), 'assigned to ')
        THEN regexp_extract(s.text, '(?i)assigned to (.*)$', 1)
      WHEN regexp_like(lower(s.text), 'marked this task complete|completed this task') THEN 'complete'
      WHEN regexp_like(lower(s.text), 'marked this task incomplete') THEN 'incomplete'
      ELSE NULL
    END AS current_value,
    CASE
      WHEN regexp_like(lower(s.text), 'moved this task from ".*" to ".*"') THEN 1
      WHEN regexp_like(lower(s.text), 'changed section from .* to .*') THEN 2
      ELSE 3
    END AS dedupe_priority
  FROM fivetran_asana.story s
  JOIN base_tasks bt
    ON bt.task_id = s.target_id
  WHERE trim(coalesce(s.text, '')) <> ''
),
    res AS (
SELECT
  task_id,
  event_at,
  changed_field,
  previous_value,
  current_value,
  text,
  story_id
FROM (
  SELECT
    hr.*,
    row_number() OVER (
      PARTITION BY
        hr.task_id,
        coalesce(hr.changed_field, ''),
        coalesce(hr.previous_value, ''),
        coalesce(hr.current_value, ''),
        date_trunc('second', hr.event_at)
      ORDER BY hr.dedupe_priority, hr.story_id
    ) AS rn
  FROM history_raw hr
  WHERE hr.changed_field IS NOT NULL
) x
WHERE 1=1
  AND rn = 1
)

SELECT *
FROM res
WHERE 1=1
  AND task_id = '1212082488015300'

;

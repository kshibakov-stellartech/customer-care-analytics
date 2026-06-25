WITH periods AS (
-- 04_processing_periods.sql
-- Processing periods per task based on status transitions in history attributes
-- Output: one row per processing period

WITH base_tasks AS (
  SELECT DISTINCT
    pt.task_id,
    t.created_at
  FROM fivetran_asana.project_task pt
  JOIN fivetran_asana.task t
    ON t.id = pt.task_id
  WHERE pt.project_id = '1211305108470489'
),
history_raw AS (
  -- Same logic as in 02_history_attrs.sql
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
history_attrs AS (
  SELECT
    task_id,
    event_at,
    changed_field,
    previous_value,
    current_value
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
  WHERE rn = 1
),
status_events AS (
  SELECT
    task_id,
    event_at,
    trim(coalesce(previous_value, '')) AS previous_status,
    trim(coalesce(current_value, '')) AS current_status,
    CASE
      WHEN lower(trim(coalesce(current_value, ''))) IN ('waiting for the result', 'done', 'can''t fix', 'cant fix', 'not relevant') THEN 'final'
      WHEN lower(trim(coalesce(current_value, ''))) IN ('backlog', 'to do', 'in progress') THEN 'work'
      ELSE NULL
    END AS current_group,
    CASE
      WHEN lower(trim(coalesce(previous_value, ''))) IN ('waiting for the result', 'done', 'can''t fix', 'cant fix', 'not relevant') THEN 'final'
      WHEN lower(trim(coalesce(previous_value, ''))) IN ('backlog', 'to do', 'in progress') THEN 'work'
      ELSE NULL
    END AS previous_group
  FROM history_attrs
  WHERE changed_field IN ('section', 'task_progress')
),
first_final_exact AS (
  -- Scenario 1: never processed before -> created_at to FIRST transition into final group
  SELECT
    x.task_id,
    x.started_at,
    x.start_status,
    se.current_status AS end_status,
    se.event_at AS ended_at
  FROM (
    SELECT
      bt.task_id,
      bt.created_at AS started_at,
      'created' AS start_status,
      MIN(se.event_at) AS ended_at
    FROM base_tasks bt
    JOIN status_events se
      ON se.task_id = bt.task_id
     AND se.current_group = 'final'
    GROUP BY bt.task_id, bt.created_at
  ) x
  JOIN status_events se
    ON se.task_id = x.task_id
   AND se.event_at = x.ended_at
   AND se.current_group = 'final'
),
reopen_starts AS (
  -- Scenario 2: final -> work (reopen) starts a new period
  SELECT
    task_id,
    event_at AS started_at,
    current_status AS start_status
  FROM status_events
  WHERE previous_group = 'final'
    AND current_group = 'work'
),
reopen_periods AS (
  SELECT
    rs.task_id,
    rs.started_at,
    rs.start_status,
    se.current_status AS end_status,
    MIN(se.event_at) AS ended_at
  FROM reopen_starts rs
  JOIN status_events se
    ON se.task_id = rs.task_id
   AND se.event_at > rs.started_at
   AND se.current_group = 'final'
  GROUP BY rs.task_id, rs.started_at, rs.start_status, se.current_status
),
reopen_exact AS (
  SELECT
    rp.task_id,
    rp.started_at,
    rp.start_status,
    se.current_status AS end_status,
    se.event_at AS ended_at
  FROM reopen_periods rp
  JOIN status_events se
    ON se.task_id = rp.task_id
   AND se.event_at = rp.ended_at
   AND se.current_group = 'final'
),
periods_union AS (
  SELECT * FROM first_final_exact
  UNION ALL
  SELECT * FROM reopen_exact
),
periods_dedup AS (
  SELECT
    task_id,
    ended_at AS event_at,
    start_status,
    started_at,
    end_status,
    ended_at,
    date_diff('minute', started_at, ended_at) AS resolution_time_min,
    row_number() OVER (
      PARTITION BY task_id, started_at, ended_at
      ORDER BY ended_at
    ) AS rn
  FROM periods_union
  WHERE ended_at > started_at
)
SELECT
  task_id,
  event_at,
  start_status,
  started_at,
  end_status,
  ended_at,
  resolution_time_min
FROM periods_dedup
WHERE rn = 1
ORDER BY task_id, started_at
), ordered AS (
  SELECT
    task_id,
    started_at,
    ended_at,
    start_status,
    end_status,
    resolution_time_min,
    LEAD(started_at) OVER (PARTITION BY task_id ORDER BY started_at, ended_at) AS next_started,
    LEAD(ended_at) OVER (PARTITION BY task_id ORDER BY started_at, ended_at) AS next_ended,
    LEAD(start_status) OVER (PARTITION BY task_id ORDER BY started_at, ended_at) AS next_start_status,
    LEAD(end_status) OVER (PARTITION BY task_id ORDER BY started_at, ended_at) AS next_end_status
  FROM periods
)
SELECT
  task_id,
  started_at,
  ended_at,
  start_status,
  end_status,
  resolution_time_min,
  next_started,
  next_ended,
  next_start_status,
  next_end_status
FROM ordered
WHERE next_started IS NOT NULL
  AND next_started < ended_at
ORDER BY task_id, started_at
LIMIT 100;

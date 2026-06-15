-- 03_task_attrs_wide.sql
-- Final: one row per task, history transposed into columns
-- Includes average processing duration per task from status transition periods

WITH base_tasks AS (
  SELECT DISTINCT
    pt.project_id,
    p.name AS project_name,
    pt.task_id,
    pt._fivetran_synced AS project_task_synced_at,
    t.created_at
  FROM fivetran_asana.project_task pt
  JOIN fivetran_asana.project p
    ON p.id = pt.project_id
  JOIN fivetran_asana.task t
    ON t.id = pt.task_id
  WHERE pt.project_id = '1211305108470489'
),
section_latest AS (
  SELECT
    ts.task_id,
    ts.section_id,
    ts._fivetran_synced AS section_assignment_synced_at,
    row_number() OVER (
      PARTITION BY ts.task_id
      ORDER BY ts._fivetran_synced DESC, ts.section_id
    ) AS rn
  FROM fivetran_asana.task_section ts
  JOIN fivetran_asana.section s
    ON s.id = ts.section_id
   AND s.project_id = '1211305108470489'
),
task_attrs AS (
  SELECT
    bt.project_id,
    bt.project_name,
    bt.project_task_synced_at,
    t.id AS task_id,
    t.name AS task_name,
    t.notes AS task_notes,
    CASE
      WHEN regexp_like(lower(coalesce(t.name, '')), 'done by auto')
        OR regexp_like(lower(coalesce(t.notes, '')), 'done by auto')
      THEN 1 ELSE 0
    END AS is_auto,
    regexp_extract(t.notes, 'tickets/([0-9]+)', 1) AS ticket_id,
    t.resource_subtype AS task_resource_subtype,
    t.parent_id AS parent_task_id,
    t.workspace_id,
    t.created_at,
    CAST(t.created_at AS date) AS created_date,
    date_trunc('week', t.created_at) AS created_week,
    t.start_on,
    t.due_on,
    t.due_at,
    t.completed,
    t.completed_at,
    CAST(t.completed_at AS date) AS completed_date,
    t.modified_at,
    t.liked,
    t.num_likes,
    t.actual_time_minutes,
    t._fivetran_deleted AS task_deleted,
    t._fivetran_synced AS task_synced_at,
    t.assignee_id,
    assignee.name AS assignee_name,
    assignee.email AS assignee_email,
    t.created_by_id,
    creator.name AS created_by_name,
    creator.email AS created_by_email,
    t.completed_by_id,
    completer.name AS completed_by_name,
    completer.email AS completed_by_email,
    sl.section_id,
    s.name AS section_name,
    s.created_at AS section_created_at,
    sl.section_assignment_synced_at,
    sl.section_assignment_synced_at AS task_section_synced_at,
    t.custom_requester,
    t.custom_team,
    t.custom_priority,
    t.custom_task_status,
    t.custom_category,
    t.custom_task_category,
    t.custom_creation_date,
    DATE_DIFF('day', t.created_at, t.completed_at) AS completion_period_days
  FROM base_tasks bt
  JOIN fivetran_asana.task t
    ON t.id = bt.task_id
  LEFT JOIN section_latest sl
    ON sl.task_id = t.id
   AND sl.rn = 1
  LEFT JOIN fivetran_asana.section s
    ON s.id = sl.section_id
  LEFT JOIN fivetran_asana.user assignee
    ON assignee.id = t.assignee_id
  LEFT JOIN fivetran_asana.user creator
    ON creator.id = t.created_by_id
  LEFT JOIN fivetran_asana.user completer
    ON completer.id = t.completed_by_id
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
history_wide AS (
  SELECT
    task_id,
    max_by(previous_value, event_at) FILTER (WHERE changed_field = 'section') AS prev_section,
    max_by(current_value, event_at) FILTER (WHERE changed_field = 'section') AS curr_section,
    max_by(previous_value, event_at) FILTER (WHERE changed_field = 'task_progress') AS prev_task_progress,
    max_by(current_value, event_at) FILTER (WHERE changed_field = 'task_progress') AS curr_task_progress,
    max_by(previous_value, event_at) FILTER (WHERE changed_field = 'assignee') AS prev_assignee,
    max_by(current_value, event_at) FILTER (WHERE changed_field = 'assignee') AS curr_assignee,
    max_by(previous_value, event_at) FILTER (WHERE changed_field = 'due_date') AS prev_due_date,
    max_by(current_value, event_at) FILTER (WHERE changed_field = 'due_date') AS curr_due_date,
    max_by(previous_value, event_at) FILTER (WHERE changed_field = 'completed_date') AS prev_completed_date,
    max_by(current_value, event_at) FILTER (WHERE changed_field = 'completed_date') AS curr_completed_date,
    max_by(previous_value, event_at) FILTER (WHERE changed_field = 'completion_status') AS prev_completion_status,
    max_by(current_value, event_at) FILTER (WHERE changed_field = 'completion_status') AS curr_completion_status,
    max(event_at) AS last_history_event_at
  FROM history_attrs
  GROUP BY task_id
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
status_events_dedup AS (
  SELECT
    task_id,
    event_at,
    previous_status,
    current_status,
    current_group,
    previous_group
  FROM (
    SELECT
      se.*,
      lag(se.previous_status) OVER (PARTITION BY se.task_id ORDER BY se.event_at) AS prev_prev_status,
      lag(se.current_status) OVER (PARTITION BY se.task_id ORDER BY se.event_at) AS prev_curr_status,
      lag(se.event_at) OVER (PARTITION BY se.task_id ORDER BY se.event_at) AS prev_event_at
    FROM status_events se
    WHERE se.current_group IS NOT NULL
  ) x
  WHERE NOT (
    x.previous_status = x.prev_prev_status
    AND x.current_status = x.prev_curr_status
    AND x.prev_event_at IS NOT NULL
    AND date_diff('second', x.prev_event_at, x.event_at) BETWEEN 0 AND 10
  )
),
group_events AS (
  SELECT
    se.task_id,
    se.event_at,
    se.current_status,
    se.current_group,
    lag(se.current_group) OVER (PARTITION BY se.task_id ORDER BY se.event_at) AS prev_group
  FROM status_events_dedup se
),
group_switches AS (
  SELECT
    task_id,
    event_at,
    current_status,
    current_group,
    prev_group
  FROM group_events
  WHERE prev_group IS NULL OR current_group <> prev_group
),
first_final_exact AS (
  SELECT
    x.task_id,
    x.started_at,
    x.start_status,
    gs.current_status AS end_status,
    gs.event_at AS ended_at
  FROM (
    SELECT
      bt.task_id,
      bt.created_at AS started_at,
      'created' AS start_status,
      MIN(gs.event_at) AS ended_at
    FROM base_tasks bt
    JOIN group_switches gs
      ON gs.task_id = bt.task_id
     AND gs.current_group = 'final'
    GROUP BY bt.task_id, bt.created_at
  ) x
  JOIN group_switches gs
    ON gs.task_id = x.task_id
   AND gs.event_at = x.ended_at
   AND gs.current_group = 'final'
),
reopen_starts AS (
  SELECT
    task_id,
    event_at AS started_at,
    current_status AS start_status
  FROM group_switches
  WHERE prev_group = 'final'
    AND current_group = 'work'
),
reopen_ends AS (
  SELECT
    task_id,
    event_at AS ended_at,
    current_status AS end_status
  FROM group_switches
  WHERE prev_group = 'work'
    AND current_group = 'final'
),
reopen_periods AS (
  SELECT
    rs.task_id,
    rs.started_at,
    rs.start_status,
    MIN(re.ended_at) AS ended_at
  FROM reopen_starts rs
  JOIN reopen_ends re
    ON re.task_id = rs.task_id
   AND re.ended_at > rs.started_at
  GROUP BY rs.task_id, rs.started_at, rs.start_status
),
reopen_exact AS (
  SELECT
    rp.task_id,
    rp.started_at,
    rp.start_status,
    re.end_status AS end_status,
    re.ended_at AS ended_at
  FROM reopen_periods rp
  JOIN reopen_ends re
    ON re.task_id = rp.task_id
   AND re.ended_at = rp.ended_at
),
periods_union AS (
  SELECT * FROM first_final_exact
  UNION ALL
  SELECT * FROM reopen_exact
),
periods_dedup AS (
  SELECT
    task_id,
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
),
processing_avg AS (
  SELECT
    task_id,
    AVG(CAST(resolution_time_min AS double)) AS avg_resolution_time_min,
    AVG(CAST(resolution_time_min AS double)) / 60.0 AS avg_resolution_time_hours,
    COUNT(*) AS processing_periods_cnt
  FROM periods_dedup
  WHERE rn = 1
  GROUP BY task_id
),
res AS (
  SELECT
    ta.*,
    hw.prev_section,
    hw.curr_section,
    hw.prev_task_progress,
    hw.curr_task_progress,
    hw.prev_assignee,
    hw.curr_assignee,
    hw.prev_due_date,
    hw.curr_due_date,
    hw.prev_completed_date,
    hw.curr_completed_date,
    hw.prev_completion_status,
    hw.curr_completion_status,
    hw.last_history_event_at,
    pa.avg_resolution_time_min,
    pa.avg_resolution_time_hours,
    pa.processing_periods_cnt
  FROM task_attrs ta
  LEFT JOIN history_wide hw
    ON hw.task_id = ta.task_id
  LEFT JOIN processing_avg pa
    ON pa.task_id = ta.task_id
)

SELECT *
FROM res
WHERE 1 = 1
  AND task_id = '1212082488015300'
;

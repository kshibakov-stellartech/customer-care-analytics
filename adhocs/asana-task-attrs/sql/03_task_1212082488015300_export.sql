SELECT *
FROM (
  -- reuse final query file directly
  WITH q AS (
    SELECT * FROM (
      SELECT * FROM fivetran_asana.project_task WHERE project_id='1211305108470489'
    )
  )
  SELECT *
  FROM (
    -- simple include: run full query from file via copy-paste omitted; use direct read by recreating from 03 in athena is not possible
    -- so here we include minimal required fields plus new avg from periods logic
    WITH base_tasks AS (
      SELECT DISTINCT pt.project_id, p.name AS project_name, pt.task_id, pt._fivetran_synced AS project_task_synced_at
      FROM fivetran_asana.project_task pt
      JOIN fivetran_asana.project p ON p.id = pt.project_id
      WHERE pt.project_id = '1211305108470489'
    ), task_attrs AS (
      SELECT bt.project_id, bt.project_name, bt.project_task_synced_at, t.*,
             regexp_extract(t.notes, 'tickets/([0-9]+)', 1) AS ticket_id
      FROM base_tasks bt
      JOIN fivetran_asana.task t ON t.id = bt.task_id
    ), history_raw AS (
      SELECT s.target_id AS task_id, s.id AS story_id, s.created_at AS event_at, s.text,
             CASE WHEN regexp_like(lower(s.text), 'moved this task from ".*" to ".*"') OR regexp_like(lower(s.text), 'changed section from .* to .*') THEN 'section'
                  WHEN regexp_like(lower(s.text), 'changed task progress') THEN 'task_progress' ELSE NULL END AS changed_field,
             CASE WHEN regexp_like(lower(s.text), 'moved this task from ".*" to ".*"') THEN regexp_extract(s.text, 'from "([^"]*)" to "', 1)
                  WHEN regexp_like(lower(s.text), 'changed section from .* to .*') THEN regexp_extract(s.text, '(?i)changed section from (.*) to ', 1)
                  WHEN regexp_like(lower(s.text), 'changed task progress from .* to .*') THEN regexp_extract(s.text, '(?i)changed task progress from (.*) to ', 1)
                  ELSE NULL END AS previous_value,
             CASE WHEN regexp_like(lower(s.text), 'moved this task from ".*" to ".*"') THEN regexp_extract(s.text, 'to "([^"]*)"', 1)
                  WHEN regexp_like(lower(s.text), 'changed section from .* to .*') THEN regexp_extract(s.text, '(?i) to (.*)$', 1)
                  WHEN regexp_like(lower(s.text), 'changed task progress from .* to .*') THEN regexp_extract(s.text, '(?i) to (.*)$', 1)
                  ELSE NULL END AS current_value
      FROM fivetran_asana.story s JOIN base_tasks bt ON bt.task_id = s.target_id
    ), status_events AS (
      SELECT task_id, event_at,
             CASE WHEN lower(trim(coalesce(current_value, ''))) IN ('waiting for the result', 'done', 'can''t fix', 'cant fix', 'not relevant') THEN 'final'
                  WHEN lower(trim(coalesce(current_value, ''))) IN ('backlog', 'to do', 'in progress') THEN 'work' ELSE NULL END AS current_group,
             CASE WHEN lower(trim(coalesce(previous_value, ''))) IN ('waiting for the result', 'done', 'can''t fix', 'cant fix', 'not relevant') THEN 'final'
                  WHEN lower(trim(coalesce(previous_value, ''))) IN ('backlog', 'to do', 'in progress') THEN 'work' ELSE NULL END AS previous_group
      FROM history_raw WHERE changed_field IN ('section','task_progress')
    ), group_events AS (
      SELECT task_id, event_at, current_group,
             lag(current_group) OVER (PARTITION BY task_id ORDER BY event_at) AS prev_group
      FROM status_events WHERE current_group IS NOT NULL
    ), group_switches AS (
      SELECT * FROM group_events WHERE prev_group IS NULL OR current_group <> prev_group
    ), first_final AS (
      SELECT bt.task_id, bt_created.created_at AS started_at, MIN(gs.event_at) AS ended_at
      FROM base_tasks bt
      JOIN fivetran_asana.task bt_created ON bt_created.id = bt.task_id
      JOIN group_switches gs ON gs.task_id = bt.task_id AND gs.current_group = 'final'
      GROUP BY bt.task_id, bt_created.created_at
    ), reopen_starts AS (
      SELECT task_id, event_at AS started_at FROM group_switches WHERE prev_group='final' AND current_group='work'
    ), reopen_ends AS (
      SELECT task_id, event_at AS ended_at FROM group_switches WHERE prev_group='work' AND current_group='final'
    ), reopen_periods AS (
      SELECT rs.task_id, rs.started_at, MIN(re.ended_at) AS ended_at
      FROM reopen_starts rs JOIN reopen_ends re ON re.task_id=rs.task_id AND re.ended_at>rs.started_at
      GROUP BY rs.task_id, rs.started_at
    ), periods AS (
      SELECT task_id, date_diff('minute', started_at, ended_at) AS resolution_time_min FROM first_final WHERE ended_at>started_at
      UNION ALL
      SELECT task_id, date_diff('minute', started_at, ended_at) AS resolution_time_min FROM reopen_periods WHERE ended_at>started_at
    ), avg_period AS (
      SELECT task_id, AVG(CAST(resolution_time_min AS double)) AS avg_resolution_time_min,
             AVG(CAST(resolution_time_min AS double))/60.0 AS avg_resolution_time_hours
      FROM periods
      GROUP BY task_id
    )
    SELECT ta.*, ap.avg_resolution_time_min, ap.avg_resolution_time_hours
    FROM task_attrs ta
    LEFT JOIN avg_period ap ON ap.task_id = ta.id
  ) t
  WHERE id = '1212082488015300'
) x;

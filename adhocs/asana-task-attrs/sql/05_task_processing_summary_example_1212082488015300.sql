-- 05_task_processing_summary.sql
-- One row per task: selected task attrs + processing metrics + is_auto + last user type

WITH base_tasks AS (
  SELECT DISTINCT
    pt.project_id,
    pt.task_id,
    t.created_at
  FROM fivetran_asana.project_task pt
  JOIN fivetran_asana.task t
    ON t.id = pt.task_id
  WHERE pt.project_id = '1211305108470489'
),
section_latest AS (
  SELECT
    ts.task_id,
    ts.section_id,
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
    t.id AS task_id,
    t.name AS task_name,
    t.notes AS task_notes,
    regexp_extract(t.notes, 'tickets/([0-9]+)', 1) AS ticket_id,
    t.created_at,
    CAST(t.created_at AS date) AS task_created_date,
    t.completed,
    assignee.name AS assignee_name,
    assignee.email AS assignee_email,
    creator.name AS created_by_name,
    creator.email AS created_by_email,
    completer.name AS completed_by_name,
    completer.email AS completed_by_email,
    s.name AS section_name,
    CASE
      WHEN regexp_like(lower(coalesce(t.name, '')), 'done by auto')
        OR regexp_like(lower(coalesce(t.notes, '')), 'done by auto')
      THEN 1 ELSE 0
    END AS is_auto
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
excluded_tag_patterns AS (
  SELECT *
  FROM (
    VALUES
      ('%cancellation_notification%'),
      ('%closed_by_merge%'),
      ('%voice_abandoned_in_voicemail%'),
      ('%appfollow%'),
      ('%spam%'),
      ('%ai_cb_triggered%'),
      ('%chargeback_precom%'),
      ('%chargeback_postcom%')
  ) AS t(pattern)
),
tickets_to_exclude AS (
  SELECT
    za.ticket_id AS ticket_to_exclude_id
  FROM data_bronze_zendesk_prod.zendesk_audit za
  JOIN excluded_tag_patterns etp
    ON za.events__field_name = 'tags'
   AND za.events__value LIKE etp.pattern
  WHERE CAST(za.created_at AS date) >= DATE '2025-01-01'
  GROUP BY 1
),
zendesk_tickets_filtered AS (
  SELECT
    za.ticket_id,
    MIN(za.created_at) AS ticket_created_at,
    CAST(MIN(za.created_at) AS date) AS ticket_created_date
  FROM data_bronze_zendesk_prod.zendesk_audit za
  LEFT JOIN tickets_to_exclude te
    ON te.ticket_to_exclude_id = za.ticket_id
  WHERE za.events__type = 'Create'
    AND za.events__field_name = 'requester_id'
    AND te.ticket_to_exclude_id IS NULL
  GROUP BY 1
  HAVING CAST(MIN(za.created_at) AS date) >= DATE '2025-01-01'
     AND CAST(MIN(za.created_at) AS date) < current_date
),
tickets_daily AS (
  SELECT
    ticket_created_date,
    COUNT(*) AS day_tickets_total
  FROM zendesk_tickets_filtered
  GROUP BY 1
),
tasks_daily AS (
  SELECT
    task_created_date,
    COUNT(*) AS day_tasks_total
  FROM task_attrs
  GROUP BY 1
),
ticket_task_daily AS (
  SELECT
    COALESCE(td.ticket_created_date, tk.task_created_date) AS created_date,
    COALESCE(td.day_tickets_total, 0) AS day_tickets_total,
    COALESCE(tk.day_tasks_total, 0) AS day_tasks_total
  FROM tickets_daily td
  FULL JOIN tasks_daily tk
    ON tk.task_created_date = td.ticket_created_date
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
    max_by(current_value, event_at) FILTER (WHERE changed_field = 'due_date') AS curr_due_date,
    max_by(previous_value, event_at) FILTER (WHERE changed_field = 'completed_date') AS prev_completed_date,
    max_by(current_value, event_at) FILTER (WHERE changed_field = 'completion_status') AS curr_completion_status,
    max_by(current_value, event_at) FILTER (WHERE regexp_like(lower(changed_field), 'user type')) AS user_type_last,
    max_by(current_value, event_at) FILTER (WHERE regexp_like(lower(changed_field), 'issue type')) AS last_issue_type,
    max_by(current_value, event_at) FILTER (WHERE regexp_like(lower(changed_field), 'issue category')) AS last_issue_category
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
    gs.current_status AS end_status,
    gs.event_at AS ended_at
  FROM (
    SELECT
      bt.task_id,
      bt.created_at AS started_at,
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
    event_at AS started_at
  FROM group_switches
  WHERE prev_group = 'final'
    AND current_group = 'work'
),
reopen_ends AS (
  SELECT
    task_id,
    event_at AS ended_at
  FROM group_switches
  WHERE prev_group = 'work'
    AND current_group = 'final'
),
reopen_periods AS (
  SELECT
    rs.task_id,
    rs.started_at,
    MIN(re.ended_at) AS ended_at
  FROM reopen_starts rs
  JOIN reopen_ends re
    ON re.task_id = rs.task_id
   AND re.ended_at > rs.started_at
  GROUP BY rs.task_id, rs.started_at
),
periods_union AS (
  SELECT task_id, started_at, ended_at FROM first_final_exact
  UNION ALL
  SELECT task_id, started_at, ended_at FROM reopen_periods
),
periods_dedup AS (
  SELECT
    task_id,
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
task_daily_enriched AS (
  SELECT
    ta.*,
    ttd.day_tickets_total,
    ttd.day_tasks_total,
    row_number() OVER (
      PARTITION BY ta.task_created_date
      ORDER BY ta.created_at, ta.task_id
    ) AS day_task_rn
  FROM task_attrs ta
  LEFT JOIN ticket_task_daily ttd
    ON ttd.created_date = ta.task_created_date
),
res AS (
  SELECT
    ta.task_id,
    ta.task_name,
    ta.task_notes,
    ta.ticket_id,
    ta.created_at,
    ta.task_created_date,
    ta.completed,
    ta.assignee_name,
    ta.assignee_email,
    ta.created_by_name,
    ta.created_by_email,
    ta.completed_by_name,
    ta.completed_by_email,
    ta.section_name,
    hw.curr_due_date,
    hw.prev_completed_date,
    hw.curr_completion_status,
    pa.avg_resolution_time_min,
    pa.avg_resolution_time_hours,
    pa.processing_periods_cnt,
    ta.is_auto,
    hw.user_type_last,
    hw.last_issue_type,
    hw.last_issue_category,
    COALESCE(ta.day_tickets_total, 0) AS day_tickets_total,
    COALESCE(ta.day_tasks_total, 0) AS day_tasks_total,
    CASE
      WHEN COALESCE(ta.day_tasks_total, 0) > 0
      THEN CAST(COALESCE(ta.day_tickets_total, 0) AS double) / CAST(ta.day_tasks_total AS double)
      ELSE 0.0
    END AS tickets_allocated_weighted,
    CASE
      WHEN ta.day_task_rn = 1 THEN COALESCE(ta.day_tickets_total, 0)
      ELSE 0
    END AS tickets_daily_anchor
  FROM task_daily_enriched ta
  LEFT JOIN history_wide hw
    ON hw.task_id = ta.task_id
  LEFT JOIN processing_avg pa
    ON pa.task_id = ta.task_id
)
SELECT
  task_id, task_name, ticket_id, task_created_date, section_name,
  avg_resolution_time_min, avg_resolution_time_hours, processing_periods_cnt,
  is_auto, user_type_last, last_issue_type, last_issue_category,
  day_tickets_total, day_tasks_total, tickets_allocated_weighted, tickets_daily_anchor
FROM res
WHERE task_id = '1212082488015300'
;

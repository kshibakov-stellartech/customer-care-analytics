-- 01_task_attrs.sql
-- One row per task (current snapshot attributes)

WITH base_tasks AS (
  SELECT DISTINCT
    pt.project_id,
    p.name AS project_name,
    pt.task_id,
    pt._fivetran_synced AS project_task_synced_at
  FROM fivetran_asana.project_task pt
  JOIN fivetran_asana.project p
    ON p.id = pt.project_id
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
    res AS (
SELECT
  bt.project_id,
  bt.project_name,
  bt.project_task_synced_at,

  t.id AS task_id,
  t.name AS task_name,
  t.notes AS task_notes,
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
)

SELECT *
FROM res
WHERE 1=1
  AND task_id = '1212082488015300'
;

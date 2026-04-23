-- Tech Support / Tech Customer Care task extraction template for Athena (fivetran_asana)
-- One row per task with assignee + sections + key lifecycle timestamps.

WITH project_pick AS (
  SELECT p.id, p.name
  FROM fivetran_asana.project p
  WHERE p._fivetran_deleted = false
    AND lower(p.name) LIKE '%tech customer care%'
  ORDER BY p.modified_at DESC
  LIMIT 1
),
base_tasks AS (
  SELECT DISTINCT
    pt.task_id,
    p.id   AS project_id,
    p.name AS project_name
  FROM project_pick p
  JOIN fivetran_asana.project_task pt
    ON p.id = pt.project_id
),
sections_agg AS (
  SELECT
    ts.task_id,
    array_join(array_agg(DISTINCT s.name), ', ') AS section_names
  FROM fivetran_asana.task_section ts
  LEFT JOIN fivetran_asana.section s
    ON s.id = ts.section_id
  GROUP BY 1
),
    res AS (
SELECT
  bt.project_id,
  bt.project_name,
  t.id                            AS task_id,
  t.name                          AS task_name,
  t.completed,
  t.created_at,
  t.completed_at,
  t.modified_at,
  t.start_on,
  t.due_on,
  t.due_at,
  COALESCE(assignee.name, '(unassigned)') AS assignee_name,
  assignee.email                  AS assignee_email,
  creator.name                    AS created_by_name,
  completer.name                  AS completed_by_name,
  COALESCE(sa.section_names, '(no section)') AS section_names,
  t.custom_requester,
  t.custom_team,
  t.custom_priority,
  t.custom_task_status,
  t.custom_category,
  t._fivetran_deleted,
  t._fivetran_synced
FROM base_tasks bt
JOIN fivetran_asana.task t
  ON t.id = bt.task_id
LEFT JOIN sections_agg sa
  ON sa.task_id = t.id
LEFT JOIN fivetran_asana.user assignee
  ON assignee.id = t.assignee_id
LEFT JOIN fivetran_asana.user creator
  ON creator.id = t.created_by_id
LEFT JOIN fivetran_asana.user completer
  ON completer.id = t.completed_by_id
ORDER BY t.modified_at DESC
)

SELECT *
FROM res
WHERE section_names = '1-Time Problem requests'

;

-- Quick checks:
-- 1) Total/open/completed
-- SELECT
--   COUNT(*) AS tasks_total,
--   COUNT_IF(completed) AS tasks_completed,
--   COUNT_IF(NOT completed) AS tasks_open,
--   MIN(created_at) AS min_created_at,
--   MAX(modified_at) AS max_modified_at
-- FROM (
--   <paste main SELECT here as subquery>
-- ) q;

-- 2) Current section load
-- SELECT section_names, COUNT(*) AS tasks_cnt
-- FROM (
--   <paste main SELECT here as subquery>
-- ) q
-- GROUP BY 1
-- ORDER BY 2 DESC;

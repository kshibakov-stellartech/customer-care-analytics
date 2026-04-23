-- 1) Find project(s) with "Tech Support" in the name
SELECT
  p.id,
  p.name,
  p.team_id,
  p.archived,
  p._fivetran_deleted,
  p.modified_at
FROM fivetran_asana.project p
WHERE lower(p.name) LIKE '%tech support%'
ORDER BY p.modified_at DESC
LIMIT 50;

-- 2) Replace <PROJECT_ID> with выбранный id из запроса выше
--    and inspect latest tasks in this project
SELECT
  p.id                         AS project_id,
  p.name                       AS project_name,
  t.id                         AS task_id,
  t.name                       AS task_name,
  t.completed,
  t.created_at,
  t.completed_at,
  t.modified_at,
  t.assignee_id,
  u.name                       AS assignee_name,
  s.id                         AS section_id,
  s.name                       AS section_name,
  t.custom_priority,
  t.custom_task_status,
  t.custom_category,
  t.custom_requester,
  t.custom_team,
  t._fivetran_deleted
FROM fivetran_asana.project p
JOIN fivetran_asana.project_task pt
  ON p.id = pt.project_id
JOIN fivetran_asana.task t
  ON pt.task_id = t.id
LEFT JOIN fivetran_asana.task_section ts
  ON t.id = ts.task_id
LEFT JOIN fivetran_asana.section s
  ON ts.section_id = s.id
LEFT JOIN fivetran_asana.user u
  ON t.assignee_id = u.id
WHERE p.id = '<PROJECT_ID>'
ORDER BY t.modified_at DESC
LIMIT 200;

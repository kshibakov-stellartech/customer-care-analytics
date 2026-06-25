WITH res AS (
  SELECT *
  FROM (
    SELECT *
    FROM (
      WITH base_tasks AS (
        SELECT DISTINCT pt.project_id, pt.task_id, t.created_at
        FROM fivetran_asana.project_task pt
        JOIN fivetran_asana.task t ON t.id = pt.task_id
        WHERE pt.project_id = '1211305108470489'
      ), section_latest AS (
        SELECT ts.task_id, ts.section_id,
               row_number() OVER (PARTITION BY ts.task_id ORDER BY ts._fivetran_synced DESC, ts.section_id) AS rn
        FROM fivetran_asana.task_section ts
        JOIN fivetran_asana.section s ON s.id = ts.section_id AND s.project_id = '1211305108470489'
      ), task_attrs AS (
        SELECT t.id AS task_id, t.name AS task_name, t.notes AS task_notes,
               regexp_extract(t.notes, 'tickets/([0-9]+)', 1) AS ticket_id,
               t.created_at, t.completed,
               assignee.name AS assignee_name, assignee.email AS assignee_email,
               creator.name AS created_by_name, creator.email AS created_by_email,
               completer.name AS completed_by_name, completer.email AS completed_by_email,
               s.name AS section_name,
               CASE WHEN regexp_like(lower(coalesce(t.name, '')), 'done by auto') OR regexp_like(lower(coalesce(t.notes, '')), 'done by auto') THEN 1 ELSE 0 END AS is_auto
        FROM base_tasks bt
        JOIN fivetran_asana.task t ON t.id = bt.task_id
        LEFT JOIN section_latest sl ON sl.task_id = t.id AND sl.rn = 1
        LEFT JOIN fivetran_asana.section s ON s.id = sl.section_id
        LEFT JOIN fivetran_asana.user assignee ON assignee.id = t.assignee_id
        LEFT JOIN fivetran_asana.user creator ON creator.id = t.created_by_id
        LEFT JOIN fivetran_asana.user completer ON completer.id = t.completed_by_id
      )
      SELECT * FROM task_attrs
    )
  )
)
SELECT COUNT(*) AS rows_total,
       COUNT(DISTINCT task_id) AS distinct_tasks,
       COUNT(*) - COUNT(DISTINCT task_id) AS dup_rows,
       SUM(CASE WHEN is_auto NOT IN (0,1) OR is_auto IS NULL THEN 1 ELSE 0 END) AS bad_is_auto
FROM res;

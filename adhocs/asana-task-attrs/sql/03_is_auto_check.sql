WITH res AS (
  SELECT *
  FROM (
    -- keep it light: use 03 query structure only for is_auto fields
    WITH base_tasks AS (
      SELECT DISTINCT pt.task_id
      FROM fivetran_asana.project_task pt
      WHERE pt.project_id = '1211305108470489'
    )
    SELECT
      t.id AS task_id,
      t.name AS task_name,
      t.notes AS task_notes,
      CASE
        WHEN regexp_like(lower(coalesce(t.name, '')), 'done by auto')
          OR regexp_like(lower(coalesce(t.notes, '')), 'done by auto')
        THEN 1 ELSE 0
      END AS is_auto
    FROM base_tasks bt
    JOIN fivetran_asana.task t ON t.id = bt.task_id
  ) x
)
SELECT
  COUNT(*) AS rows_total,
  SUM(is_auto) AS is_auto_cnt,
  SUM(CASE WHEN is_auto = 1 AND regexp_like(lower(coalesce(task_name, '') || ' ' || coalesce(task_notes, '')), 'done by auto') THEN 1 ELSE 0 END) AS matched_cnt
FROM res;

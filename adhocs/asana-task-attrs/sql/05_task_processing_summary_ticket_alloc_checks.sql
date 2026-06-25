WITH excluded_tag_patterns AS (
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
  SELECT za.ticket_id AS ticket_to_exclude_id
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
  SELECT ticket_created_date, COUNT(*) AS day_tickets_total
  FROM zendesk_tickets_filtered
  GROUP BY 1
),
tasks_daily AS (
  SELECT CAST(t.created_at AS date) AS task_created_date, COUNT(*) AS day_tasks_total
  FROM fivetran_asana.project_task pt
  JOIN fivetran_asana.task t ON t.id = pt.task_id
  WHERE pt.project_id = '1211305108470489'
  GROUP BY 1
),
joined AS (
  SELECT
    tk.task_created_date,
    tk.day_tasks_total,
    COALESCE(td.day_tickets_total, 0) AS day_tickets_total,
    CAST(COALESCE(td.day_tickets_total, 0) AS double) / CAST(tk.day_tasks_total AS double) AS per_task_weight
  FROM tasks_daily tk
  LEFT JOIN tickets_daily td
    ON td.ticket_created_date = tk.task_created_date
)
SELECT
  SUM(day_tickets_total) AS sum_day_tickets_total,
  SUM(per_task_weight * day_tasks_total) AS sum_weighted_rebuilt,
  SUM(day_tickets_total) - SUM(per_task_weight * day_tasks_total) AS diff_should_be_zero,
  COUNT(*) AS days_with_tasks
FROM joined

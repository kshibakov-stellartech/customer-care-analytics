WITH tickets_source AS (
    SELECT DISTINCT
        z.ticket_id,
        CAST(DATE_TRUNC('week', z.created_at) AS DATE) AS week_dt
    FROM data_bronze_zendesk_prod.zendesk_tickets z
),

audit_source AS (
    SELECT DISTINCT
        za.ticket_id,
        CAST(DATE_TRUNC('week', z.created_at) AS DATE) AS week_dt
    FROM data_bronze_zendesk_prod.zendesk_audit za
    JOIN data_bronze_zendesk_prod.zendesk_tickets z
      ON z.ticket_id = za.ticket_id
),

weekly AS (
    SELECT
        COALESCE(a.week_dt, t.week_dt) AS week_dt,
        COUNT(DISTINCT a.ticket_id) AS audit_ticket_cnt,
        COUNT(DISTINCT t.ticket_id) AS tickets_ticket_cnt,
        COUNT(DISTINCT CASE WHEN a.ticket_id IS NOT NULL AND t.ticket_id IS NULL THEN a.ticket_id END) AS only_in_audit,
        COUNT(DISTINCT CASE WHEN t.ticket_id IS NOT NULL AND a.ticket_id IS NULL THEN t.ticket_id END) AS only_in_tickets
    FROM audit_source a
    FULL OUTER JOIN tickets_source t
      ON a.ticket_id = t.ticket_id
    GROUP BY 1
)
SELECT *
FROM weekly
WHERE only_in_audit > 0 OR only_in_tickets > 0
ORDER BY week_dt DESC
LIMIT 40;

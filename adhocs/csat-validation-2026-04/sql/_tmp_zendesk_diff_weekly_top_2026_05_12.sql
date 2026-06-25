WITH
excluded_tag_patterns AS (
    SELECT * FROM (
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
excluded_tickets AS (
    SELECT DISTINCT za.ticket_id
    FROM data_bronze_zendesk_prod.zendesk_audit za
    JOIN data_bronze_zendesk_prod.zendesk_tickets z ON z.ticket_id = za.ticket_id
    JOIN excluded_tag_patterns etp
      ON za.events__field_name = 'tags'
     AND za.events__value LIKE etp.pattern
    WHERE CAST(z.created_at AS DATE) >= DATE '2025-11-01'
      AND CAST(z.created_at AS DATE) < current_date
),
tickets_source AS (
    SELECT DISTINCT z.ticket_id, CAST(DATE_TRUNC('week', z.created_at) AS DATE) AS week_dt
    FROM data_bronze_zendesk_prod.zendesk_tickets z
    LEFT JOIN excluded_tickets et ON et.ticket_id = z.ticket_id
    WHERE CAST(z.created_at AS DATE) >= DATE '2025-11-01'
      AND CAST(z.created_at AS DATE) < current_date
      AND et.ticket_id IS NULL
),
audit_source AS (
    SELECT DISTINCT za.ticket_id, CAST(DATE_TRUNC('week', z.created_at) AS DATE) AS week_dt
    FROM data_bronze_zendesk_prod.zendesk_audit za
    JOIN data_bronze_zendesk_prod.zendesk_tickets z ON z.ticket_id = za.ticket_id
    LEFT JOIN excluded_tickets et ON et.ticket_id = za.ticket_id
    WHERE CAST(z.created_at AS DATE) >= DATE '2025-11-01'
      AND CAST(z.created_at AS DATE) < current_date
      AND et.ticket_id IS NULL
),
weekly AS (
    SELECT
      COALESCE(a.week_dt, t.week_dt) AS week_dt,
      COUNT(DISTINCT a.ticket_id) AS audit_ticket_cnt,
      COUNT(DISTINCT t.ticket_id) AS tickets_ticket_cnt,
      COUNT(DISTINCT CASE WHEN a.ticket_id IS NOT NULL AND t.ticket_id IS NULL THEN a.ticket_id END) AS only_in_audit,
      COUNT(DISTINCT CASE WHEN t.ticket_id IS NOT NULL AND a.ticket_id IS NULL THEN t.ticket_id END) AS only_in_tickets
    FROM audit_source a
    FULL OUTER JOIN tickets_source t ON a.ticket_id = t.ticket_id
    GROUP BY 1
)
SELECT week_dt, audit_ticket_cnt, tickets_ticket_cnt, only_in_audit, only_in_tickets
FROM weekly
WHERE only_in_audit > 0 OR only_in_tickets > 0
ORDER BY only_in_tickets DESC, week_dt DESC
LIMIT 20;

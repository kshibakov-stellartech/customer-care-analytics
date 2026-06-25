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
    SELECT DISTINCT z.ticket_id, CAST(z.created_at AS DATE) AS created_dt, CAST(DATE_TRUNC('week', z.created_at) AS DATE) AS week_dt
    FROM data_bronze_zendesk_prod.zendesk_tickets z
    LEFT JOIN excluded_tickets et ON et.ticket_id = z.ticket_id
    WHERE CAST(z.created_at AS DATE) >= DATE '2025-11-01'
      AND CAST(z.created_at AS DATE) < current_date
      AND et.ticket_id IS NULL
),
audit_source AS (
    SELECT DISTINCT za.ticket_id
    FROM data_bronze_zendesk_prod.zendesk_audit za
    JOIN data_bronze_zendesk_prod.zendesk_tickets z ON z.ticket_id = za.ticket_id
    LEFT JOIN excluded_tickets et ON et.ticket_id = za.ticket_id
    WHERE CAST(z.created_at AS DATE) >= DATE '2025-11-01'
      AND CAST(z.created_at AS DATE) < current_date
      AND et.ticket_id IS NULL
)
SELECT t.week_dt, t.created_dt, t.ticket_id
FROM tickets_source t
LEFT JOIN audit_source a ON a.ticket_id = t.ticket_id
WHERE a.ticket_id IS NULL
ORDER BY t.created_dt DESC
LIMIT 30;

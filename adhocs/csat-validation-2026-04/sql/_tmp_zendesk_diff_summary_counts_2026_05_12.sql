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
excluded_by_audit_date AS (
    SELECT DISTINCT za.ticket_id
    FROM data_bronze_zendesk_prod.zendesk_audit za
    JOIN excluded_tag_patterns etp
      ON za.events__field_name = 'tags'
     AND za.events__value LIKE etp.pattern
    WHERE za.created_at >= DATE '2025-11-01'
),
excluded_by_ticket_date AS (
    SELECT DISTINCT za.ticket_id
    FROM data_bronze_zendesk_prod.zendesk_audit za
    JOIN data_bronze_zendesk_prod.zendesk_tickets z ON z.ticket_id = za.ticket_id
    JOIN excluded_tag_patterns etp
      ON za.events__field_name = 'tags'
     AND za.events__value LIKE etp.pattern
    WHERE CAST(z.created_at AS DATE) >= DATE '2025-11-01'
      AND CAST(z.created_at AS DATE) < current_date
),
tickets_base AS (
    SELECT z.ticket_id, CAST(z.created_at AS DATE) AS created_dt
    FROM data_bronze_zendesk_prod.zendesk_tickets z
    WHERE CAST(z.created_at AS DATE) >= DATE '2025-11-01'
),
tickets_base_bounded AS (
    SELECT * FROM tickets_base WHERE created_dt < current_date
),
v1_unbounded AS (
    SELECT DISTINCT t.ticket_id
    FROM tickets_base t
    LEFT JOIN excluded_by_audit_date e ON e.ticket_id = t.ticket_id
    WHERE e.ticket_id IS NULL
),
v1_bounded AS (
    SELECT DISTINCT t.ticket_id
    FROM tickets_base_bounded t
    LEFT JOIN excluded_by_audit_date e ON e.ticket_id = t.ticket_id
    WHERE e.ticket_id IS NULL
),
v2_tickets AS (
    SELECT DISTINCT t.ticket_id
    FROM tickets_base_bounded t
    LEFT JOIN excluded_by_ticket_date e ON e.ticket_id = t.ticket_id
    WHERE e.ticket_id IS NULL
),
v2_audit AS (
    SELECT DISTINCT za.ticket_id
    FROM data_bronze_zendesk_prod.zendesk_audit za
    JOIN data_bronze_zendesk_prod.zendesk_tickets z ON z.ticket_id = za.ticket_id
    LEFT JOIN excluded_by_ticket_date e ON e.ticket_id = za.ticket_id
    WHERE CAST(z.created_at AS DATE) >= DATE '2025-11-01'
      AND CAST(z.created_at AS DATE) < current_date
      AND e.ticket_id IS NULL
)
SELECT * FROM (
    SELECT 'excluded_by_audit_date' AS metric, COUNT(*) AS value FROM excluded_by_audit_date
    UNION ALL SELECT 'excluded_by_ticket_date', COUNT(*) FROM excluded_by_ticket_date
    UNION ALL SELECT 'v1_unbounded', COUNT(*) FROM v1_unbounded
    UNION ALL SELECT 'v1_bounded', COUNT(*) FROM v1_bounded
    UNION ALL SELECT 'v2_tickets', COUNT(*) FROM v2_tickets
    UNION ALL SELECT 'v2_audit', COUNT(*) FROM v2_audit
    UNION ALL SELECT 'delta_v1_unbounded_minus_v1_bounded', (SELECT COUNT(*) FROM v1_unbounded) - (SELECT COUNT(*) FROM v1_bounded)
    UNION ALL SELECT 'delta_v1_bounded_minus_v2_tickets', (SELECT COUNT(*) FROM v1_bounded) - (SELECT COUNT(*) FROM v2_tickets)
    UNION ALL SELECT 'delta_v2_tickets_minus_v2_audit', (SELECT COUNT(*) FROM v2_tickets) - (SELECT COUNT(*) FROM v2_audit)
) t
ORDER BY metric;

WITH
excluded_tag_patterns AS (
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

-- Excluded logic A: by audit event date (as in 1st query)
excluded_by_audit_date AS (
    SELECT DISTINCT za.ticket_id
    FROM data_bronze_zendesk_prod.zendesk_audit za
    JOIN excluded_tag_patterns etp
      ON za.events__field_name = 'tags'
     AND za.events__value LIKE etp.pattern
    WHERE za.created_at >= DATE '2025-11-01'
),

-- Excluded logic B: by ticket created date (as in 2nd query)
excluded_by_ticket_date AS (
    SELECT DISTINCT za.ticket_id
    FROM data_bronze_zendesk_prod.zendesk_audit za
    JOIN data_bronze_zendesk_prod.zendesk_tickets z
      ON z.ticket_id = za.ticket_id
    JOIN excluded_tag_patterns etp
      ON za.events__field_name = 'tags'
     AND za.events__value LIKE etp.pattern
    WHERE CAST(z.created_at AS DATE) >= DATE '2025-11-01'
      AND CAST(z.created_at AS DATE) < current_date
),

tickets_base AS (
    SELECT
      z.ticket_id,
      CAST(z.created_at AS DATE) AS created_dt,
      CAST(DATE_TRUNC('week', z.created_at) AS DATE) AS week_dt
    FROM data_bronze_zendesk_prod.zendesk_tickets z
    WHERE CAST(z.created_at AS DATE) >= DATE '2025-11-01'
),

tickets_base_bounded AS (
    SELECT *
    FROM tickets_base
    WHERE created_dt < current_date
),

-- version 1 count (no upper bound, excluded by audit date)
v1_unbounded AS (
    SELECT DISTINCT t.ticket_id, t.week_dt
    FROM tickets_base t
    LEFT JOIN excluded_by_audit_date e ON e.ticket_id = t.ticket_id
    WHERE e.ticket_id IS NULL
),

-- version 1 but with upper bound aligned
v1_bounded AS (
    SELECT DISTINCT t.ticket_id, t.week_dt
    FROM tickets_base_bounded t
    LEFT JOIN excluded_by_audit_date e ON e.ticket_id = t.ticket_id
    WHERE e.ticket_id IS NULL
),

-- version 2 tickets_source (bounded, excluded by ticket date)
v2_tickets_source AS (
    SELECT DISTINCT t.ticket_id, t.week_dt
    FROM tickets_base_bounded t
    LEFT JOIN excluded_by_ticket_date e ON e.ticket_id = t.ticket_id
    WHERE e.ticket_id IS NULL
),

-- version 2 audit_source (bounded, excluded by ticket date)
v2_audit_source AS (
    SELECT DISTINCT
      za.ticket_id,
      CAST(DATE_TRUNC('week', z.created_at) AS DATE) AS week_dt
    FROM data_bronze_zendesk_prod.zendesk_audit za
    JOIN data_bronze_zendesk_prod.zendesk_tickets z
      ON z.ticket_id = za.ticket_id
    LEFT JOIN excluded_by_ticket_date e
      ON e.ticket_id = za.ticket_id
    WHERE CAST(z.created_at AS DATE) >= DATE '2025-11-01'
      AND CAST(z.created_at AS DATE) < current_date
      AND e.ticket_id IS NULL
),

summary AS (
    SELECT 'v1_unbounded' AS check_name, COUNT(DISTINCT ticket_id) AS ticket_cnt FROM v1_unbounded
    UNION ALL
    SELECT 'v1_bounded', COUNT(DISTINCT ticket_id) FROM v1_bounded
    UNION ALL
    SELECT 'v2_tickets_source', COUNT(DISTINCT ticket_id) FROM v2_tickets_source
    UNION ALL
    SELECT 'v2_audit_source', COUNT(DISTINCT ticket_id) FROM v2_audit_source
    UNION ALL
    SELECT 'excluded_by_audit_date', COUNT(DISTINCT ticket_id) FROM excluded_by_audit_date
    UNION ALL
    SELECT 'excluded_by_ticket_date', COUNT(DISTINCT ticket_id) FROM excluded_by_ticket_date
),

weekly_compare_v2 AS (
    SELECT
      COALESCE(a.week_dt, t.week_dt) AS week_dt,
      COUNT(DISTINCT a.ticket_id) AS audit_ticket_cnt,
      COUNT(DISTINCT t.ticket_id) AS tickets_ticket_cnt,
      COUNT(DISTINCT CASE WHEN a.ticket_id IS NOT NULL AND t.ticket_id IS NULL THEN a.ticket_id END) AS only_in_audit,
      COUNT(DISTINCT CASE WHEN t.ticket_id IS NOT NULL AND a.ticket_id IS NULL THEN t.ticket_id END) AS only_in_tickets,
      COUNT(DISTINCT CASE WHEN t.ticket_id IS NOT NULL AND a.ticket_id IS NOT NULL THEN t.ticket_id END) AS in_both
    FROM v2_audit_source a
    FULL OUTER JOIN v2_tickets_source t
      ON a.ticket_id = t.ticket_id
    GROUP BY 1
),

weekly_v1_bound_vs_v2_tickets AS (
    SELECT
      COALESCE(v1.week_dt, v2.week_dt) AS week_dt,
      COUNT(DISTINCT v1.ticket_id) AS v1_bounded_cnt,
      COUNT(DISTINCT v2.ticket_id) AS v2_tickets_cnt,
      COUNT(DISTINCT CASE WHEN v1.ticket_id IS NOT NULL AND v2.ticket_id IS NULL THEN v1.ticket_id END) AS only_in_v1_bounded,
      COUNT(DISTINCT CASE WHEN v2.ticket_id IS NOT NULL AND v1.ticket_id IS NULL THEN v2.ticket_id END) AS only_in_v2_tickets
    FROM v1_bounded v1
    FULL OUTER JOIN v2_tickets_source v2
      ON v1.ticket_id = v2.ticket_id
    GROUP BY 1
)

SELECT 'summary' AS section, check_name AS c1, CAST(ticket_cnt AS VARCHAR) AS c2, NULL AS c3, NULL AS c4, NULL AS c5
FROM summary

UNION ALL

SELECT 'weekly_v2' AS section,
       CAST(week_dt AS VARCHAR) AS c1,
       CAST(audit_ticket_cnt AS VARCHAR) AS c2,
       CAST(tickets_ticket_cnt AS VARCHAR) AS c3,
       CAST(only_in_audit AS VARCHAR) AS c4,
       CAST(only_in_tickets AS VARCHAR) AS c5
FROM weekly_compare_v2

UNION ALL

SELECT 'weekly_v1b_vs_v2t' AS section,
       CAST(week_dt AS VARCHAR) AS c1,
       CAST(v1_bounded_cnt AS VARCHAR) AS c2,
       CAST(v2_tickets_cnt AS VARCHAR) AS c3,
       CAST(only_in_v1_bounded AS VARCHAR) AS c4,
       CAST(only_in_v2_tickets AS VARCHAR) AS c5
FROM weekly_v1_bound_vs_v2_tickets

ORDER BY 1, 2;

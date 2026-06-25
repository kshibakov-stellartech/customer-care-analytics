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

tickets_to_exclude AS (
    SELECT
        za.ticket_id AS ticket_to_exclude_id,
        MIN(CAST(za.created_at AS DATE)) AS created_date
    FROM data_bronze_zendesk_prod.zendesk_audit za
    JOIN excluded_tag_patterns etp
      ON za.events__field_name = 'tags'
     AND za.events__value LIKE etp.pattern
    WHERE za.created_at >= DATE '2026-01-01'
    GROUP BY 1
),

tickets AS (
    SELECT
        za.ticket_id,
        MIN(za.created_at) AS ticket_created_at,
        CAST(MAX(za.events__value) AS BIGINT) AS requester_id
    FROM data_bronze_zendesk_prod.zendesk_audit za
    WHERE za.events__type = 'Create'
      AND za.events__field_name = 'requester_id'
      AND NOT EXISTS (
          SELECT 1
          FROM tickets_to_exclude te
          WHERE te.ticket_to_exclude_id = za.ticket_id
      )
    GROUP BY 1
    HAVING MIN(CAST(za.created_at AS DATE)) >= DATE '2026-01-01'
       AND MIN(CAST(za.created_at AS DATE)) < current_date
),

source_tickets_table AS (
    SELECT DISTINCT
        t.ticket_id,
        CAST(DATE_TRUNC('week', t.ticket_created_at) AS DATE) AS week_dt
    FROM tickets t
    JOIN data_bronze_zendesk_prod.zendesk_tickets zt
      ON zt.ticket_id = t.ticket_id
),

source_audit_table AS (
    SELECT DISTINCT
        t.ticket_id,
        CAST(DATE_TRUNC('week', t.ticket_created_at) AS DATE) AS week_dt
    FROM tickets t
    JOIN data_bronze_zendesk_prod.zendesk_audit za
      ON za.ticket_id = t.ticket_id
),

weekly_comparison AS (
    SELECT
        COALESCE(a.week_dt, tt.week_dt) AS week_dt,
        COUNT(DISTINCT a.ticket_id) AS audit_ticket_cnt,
        COUNT(DISTINCT tt.ticket_id) AS tickets_table_ticket_cnt,
        COUNT(DISTINCT CASE WHEN a.ticket_id IS NOT NULL AND tt.ticket_id IS NULL THEN a.ticket_id END) AS only_in_audit,
        COUNT(DISTINCT CASE WHEN tt.ticket_id IS NOT NULL AND a.ticket_id IS NULL THEN tt.ticket_id END) AS only_in_tickets_table,
        COUNT(DISTINCT CASE WHEN a.ticket_id IS NOT NULL AND tt.ticket_id IS NOT NULL THEN tt.ticket_id END) AS in_both
    FROM source_audit_table a
    FULL OUTER JOIN source_tickets_table tt
      ON a.ticket_id = tt.ticket_id
    GROUP BY 1
),

total_comparison AS (
    SELECT
        'TOTAL' AS row_type,
        NULL AS week_dt,
        COUNT(DISTINCT a.ticket_id) AS audit_ticket_cnt,
        COUNT(DISTINCT tt.ticket_id) AS tickets_table_ticket_cnt,
        COUNT(DISTINCT CASE WHEN a.ticket_id IS NOT NULL AND tt.ticket_id IS NULL THEN a.ticket_id END) AS only_in_audit,
        COUNT(DISTINCT CASE WHEN tt.ticket_id IS NOT NULL AND a.ticket_id IS NULL THEN tt.ticket_id END) AS only_in_tickets_table,
        COUNT(DISTINCT CASE WHEN a.ticket_id IS NOT NULL AND tt.ticket_id IS NOT NULL THEN tt.ticket_id END) AS in_both
    FROM source_audit_table a
    FULL OUTER JOIN source_tickets_table tt
      ON a.ticket_id = tt.ticket_id
)

SELECT
    'TOTAL' AS section,
    CAST(audit_ticket_cnt AS VARCHAR) AS c1,
    CAST(tickets_table_ticket_cnt AS VARCHAR) AS c2,
    CAST(only_in_audit AS VARCHAR) AS c3,
    CAST(only_in_tickets_table AS VARCHAR) AS c4,
    CAST(in_both AS VARCHAR) AS c5,
    NULL AS c6
FROM total_comparison

UNION ALL

SELECT
    'WEEKLY' AS section,
    CAST(week_dt AS VARCHAR) AS c1,
    CAST(audit_ticket_cnt AS VARCHAR) AS c2,
    CAST(tickets_table_ticket_cnt AS VARCHAR) AS c3,
    CAST(only_in_audit AS VARCHAR) AS c4,
    CAST(only_in_tickets_table AS VARCHAR) AS c5,
    CAST(in_both AS VARCHAR) AS c6
FROM weekly_comparison
ORDER BY 1, 2;

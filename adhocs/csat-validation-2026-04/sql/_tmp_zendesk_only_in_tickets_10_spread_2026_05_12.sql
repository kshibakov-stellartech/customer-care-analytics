WITH tickets_source AS (
    SELECT DISTINCT
        z.ticket_id,
        CAST(z.created_at AS DATE) AS created_dt,
        CAST(DATE_TRUNC('week', z.created_at) AS DATE) AS week_dt
    FROM data_bronze_zendesk_prod.zendesk_tickets z
),

audit_source AS (
    SELECT DISTINCT
        za.ticket_id
    FROM data_bronze_zendesk_prod.zendesk_audit za
),

only_in_tickets AS (
    SELECT
        t.ticket_id,
        t.created_dt,
        t.week_dt
    FROM tickets_source t
    LEFT JOIN audit_source a
      ON a.ticket_id = t.ticket_id
    WHERE a.ticket_id IS NULL
),

bucketed AS (
    SELECT
        ticket_id,
        created_dt,
        week_dt,
        NTILE(10) OVER (ORDER BY created_dt) AS bucket
    FROM only_in_tickets
),

ranked AS (
    SELECT
        ticket_id,
        created_dt,
        week_dt,
        bucket,
        ROW_NUMBER() OVER (
            PARTITION BY bucket
            ORDER BY created_dt, ticket_id
        ) AS rn
    FROM bucketed
)

SELECT
    bucket,
    created_dt,
    week_dt,
    ticket_id
FROM ranked
WHERE rn = 1
ORDER BY created_dt;

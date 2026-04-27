WITH tickets AS (
    SELECT
        za.ticket_id,
        CAST(MAX(za.events__value) AS BIGINT) AS requester_id
    FROM data_bronze_zendesk_prod.zendesk_audit za
    WHERE za.events__type = 'Create'
      AND za.events__field_name = 'requester_id'
      AND za.created_at >= DATE '2026-01-01'
    GROUP BY 1
),
base AS (
    SELECT
        za.ticket_id,
        date_add('hour', 2, za.created_at) AS created_at,
        za.events__id,
        za.events__type,
        za.events__field_name,
        za.events__value,
        za.events__public,
        za.events__from_title,
        TRY_CAST(CAST(za.author_id AS DOUBLE) AS BIGINT) AS author_id,
        t.requester_id,
        CASE
            WHEN za.events__type = 'Notification' AND za.events__from_title IN (
                'Auto_12: Auto-reply to refund requests (Stores)',
                'Auto_21: Auto-reply to delete+refund requests (Paddle/PayPal)',
                'Auto_91: Auto-reply to delete requests (Stores)',
                'Auto_13: Auto-reply to refund requests (Paddle/PayPal)',
                'Auto_29: Auto-reply - payment_not_found AI',
                'Auto_29: Auto-reply - payment_not_found AI (2nd)',
                'Auto_29: Auto-reply - payment_not_found (automation failed)',
                'Auto_35: Auto-reply to delete+refund requests (threats/risk)',
                'Auto_6: Auto-reply to cancel requests (Web) ',
                'Auto_7: Auto-reply to cancel requests (Stores)',
                'Auto_28: Freemium only - payment_not_found',
                'Auto-reply - something is wrong with my subscription - SmartyMe'
            ) THEN 1
            WHEN TRY_CAST(CAST(za.events__author_id AS DOUBLE) AS BIGINT) IS NOT NULL
                 AND za.events__public = TRUE THEN 2
            ELSE 0
        END AS is_public_communication
    FROM data_bronze_zendesk_prod.zendesk_audit za
    JOIN tickets t ON t.ticket_id = za.ticket_id
),
raw_csat AS (
    SELECT
        ticket_id,
        created_at,
        events__id,
        author_id,
        events__public,
        events__type,
        events__field_name,
        events__value,
        LEAD(events__field_name) OVER (PARTITION BY ticket_id ORDER BY created_at) AS csat_flag,
        LEAD(events__value) OVER (PARTITION BY ticket_id ORDER BY created_at) AS csat_val
    FROM base
    WHERE (events__type = 'Comment' AND author_id <> requester_id)
       OR (events__type = 'Change' AND events__field_name = 'satisfaction_score' AND events__value IN ('good','bad'))
),
csat_attr AS (
    SELECT
        ticket_id,
        created_at,
        events__id,
        events__public,
        csat_val,
        ROW_NUMBER() OVER (PARTITION BY ticket_id ORDER BY created_at DESC) AS csat_rn
    FROM raw_csat
    WHERE events__type = 'Comment'
      AND csat_flag = 'satisfaction_score'
),
latest AS (
    SELECT *
    FROM csat_attr
    WHERE csat_rn = 1
),
valid_message_events AS (
    SELECT DISTINCT ticket_id, created_at
    FROM base
    WHERE (events__type = 'Comment' AND events__public = TRUE)
       OR (events__type = 'Notification' AND is_public_communication IN (1,2))
)
SELECT
    COUNT(*) AS latest_csat_rows,
    SUM(CASE WHEN COALESCE(events__public, false) = false THEN 1 ELSE 0 END) AS latest_csat_on_private_comment,
    SUM(CASE WHEN vme.ticket_id IS NOT NULL THEN 1 ELSE 0 END) AS latest_csat_timestamp_has_valid_message_event,
    SUM(CASE WHEN vme.ticket_id IS NULL THEN 1 ELSE 0 END) AS latest_csat_timestamp_without_valid_message_event
FROM latest l
LEFT JOIN valid_message_events vme
  ON vme.ticket_id = l.ticket_id
 AND vme.created_at = l.created_at

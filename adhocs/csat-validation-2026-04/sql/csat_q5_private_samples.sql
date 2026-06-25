WITH tickets AS (
    SELECT ticket_id, CAST(MAX(events__value) AS BIGINT) AS requester_id
    FROM data_bronze_zendesk_prod.zendesk_audit
    WHERE events__type = 'Create'
      AND events__field_name = 'requester_id'
      AND created_at >= DATE '2026-01-01'
    GROUP BY 1
),
rel AS (
    SELECT
        za.ticket_id,
        za.created_at,
        za.events__id,
        za.events__type,
        za.events__field_name,
        za.events__value,
        za.events__public,
        TRY_CAST(CAST(za.author_id AS DOUBLE) AS BIGINT) AS author_id,
        t.requester_id
    FROM data_bronze_zendesk_prod.zendesk_audit za
    JOIN tickets t ON t.ticket_id = za.ticket_id
    WHERE (za.events__type = 'Comment' AND TRY_CAST(CAST(za.author_id AS DOUBLE) AS BIGINT) <> t.requester_id)
       OR (za.events__type = 'Change' AND za.events__field_name = 'satisfaction_score' AND za.events__value IN ('good','bad'))
),
seq AS (
    SELECT
        *,
        LEAD(events__field_name) OVER (PARTITION BY ticket_id ORDER BY created_at) AS csat_flag,
        LEAD(events__value) OVER (PARTITION BY ticket_id ORDER BY created_at) AS csat_val,
        LEAD(events__id) OVER (PARTITION BY ticket_id ORDER BY created_at) AS csat_event_id
    FROM rel
)
SELECT
    ticket_id,
    created_at AS comment_created_at,
    events__id AS comment_event_id,
    events__public,
    csat_event_id,
    csat_val
FROM seq
WHERE events__type = 'Comment'
  AND csat_flag = 'satisfaction_score'
  AND COALESCE(events__public, false) = false
ORDER BY created_at DESC
LIMIT 30

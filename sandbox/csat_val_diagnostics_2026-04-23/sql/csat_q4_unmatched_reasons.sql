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
        LEAD(events__field_name) OVER (PARTITION BY ticket_id ORDER BY created_at) AS lead_flag_old,
        LEAD(events__id) OVER (PARTITION BY ticket_id ORDER BY created_at) AS lead_id_old,
        LAG(events__type) OVER (PARTITION BY ticket_id ORDER BY created_at, events__id) AS prev_type,
        LAG(events__field_name) OVER (PARTITION BY ticket_id ORDER BY created_at, events__id) AS prev_field,
        LAG(events__public) OVER (PARTITION BY ticket_id ORDER BY created_at, events__id) AS prev_public
    FROM rel
),
matched_scores AS (
    SELECT DISTINCT ticket_id, lead_id_old AS events__id
    FROM seq
    WHERE events__type = 'Comment'
      AND lead_flag_old = 'satisfaction_score'
      AND lead_id_old IS NOT NULL
),
unmatched_scores AS (
    SELECT s.*
    FROM seq s
    LEFT JOIN matched_scores m
      ON m.ticket_id = s.ticket_id
     AND m.events__id = s.events__id
    WHERE s.events__type = 'Change'
      AND s.events__field_name = 'satisfaction_score'
      AND s.events__value IN ('good','bad')
      AND m.events__id IS NULL
)
SELECT
    COALESCE(prev_type, 'NULL') AS prev_type,
    COALESCE(prev_field, 'NULL') AS prev_field,
    COALESCE(CAST(prev_public AS VARCHAR), 'NULL') AS prev_public,
    COUNT(*) AS cnt
FROM unmatched_scores
GROUP BY 1,2,3
ORDER BY cnt DESC
LIMIT 20

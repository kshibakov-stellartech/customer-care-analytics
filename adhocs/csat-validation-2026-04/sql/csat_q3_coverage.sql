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
        LEAD(events__id) OVER (PARTITION BY ticket_id ORDER BY created_at) AS lead_id_old
    FROM rel
),
score_events AS (
    SELECT ticket_id, events__id, events__value
    FROM rel
    WHERE events__type = 'Change' AND events__field_name = 'satisfaction_score' AND events__value IN ('good','bad')
),
matched_scores AS (
    SELECT DISTINCT ticket_id, lead_id_old AS events__id
    FROM seq
    WHERE events__type = 'Comment'
      AND lead_flag_old = 'satisfaction_score'
      AND lead_id_old IS NOT NULL
),
score_per_ticket AS (
    SELECT ticket_id, COUNT(*) AS score_cnt
    FROM score_events
    GROUP BY 1
)
SELECT
    (SELECT COUNT(*) FROM score_events) AS total_score_events,
    (SELECT COUNT(DISTINCT ticket_id) FROM score_events) AS tickets_with_score,
    (SELECT COUNT(*) FROM score_per_ticket WHERE score_cnt > 1) AS tickets_with_multiple_scores,
    (SELECT COUNT(*) FROM matched_scores) AS score_events_matched_by_current_logic,
    (SELECT COUNT(*)
     FROM score_events s
     LEFT JOIN matched_scores m
       ON m.ticket_id = s.ticket_id
      AND m.events__id = s.events__id
     WHERE m.events__id IS NULL) AS score_events_unmatched_by_current_logic

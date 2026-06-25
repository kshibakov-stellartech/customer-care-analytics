WITH tickets AS (
    SELECT
        ticket_id,
        CAST(MAX(events__value) AS BIGINT) AS requester_id
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
        LEAD(events__value) OVER (PARTITION BY ticket_id ORDER BY created_at) AS lead_val_old,
        LEAD(events__id) OVER (PARTITION BY ticket_id ORDER BY created_at) AS lead_id_old,
        LEAD(events__field_name) OVER (PARTITION BY ticket_id ORDER BY created_at, events__id) AS lead_flag_new,
        LEAD(events__value) OVER (PARTITION BY ticket_id ORDER BY created_at, events__id) AS lead_val_new,
        LEAD(events__id) OVER (PARTITION BY ticket_id ORDER BY created_at, events__id) AS lead_id_new
    FROM rel
),
comment_rows AS (
    SELECT *
    FROM seq
    WHERE events__type = 'Comment'
)
SELECT
    COUNT(*) AS total_agent_comments_in_scope,
    SUM(CASE WHEN lead_flag_old = 'satisfaction_score' THEN 1 ELSE 0 END) AS matched_old_logic,
    SUM(CASE WHEN lead_flag_new = 'satisfaction_score' THEN 1 ELSE 0 END) AS matched_new_logic,
    SUM(CASE WHEN lead_flag_old = 'satisfaction_score' AND COALESCE(events__public, false) = false THEN 1 ELSE 0 END) AS matched_on_private_comment_old,
    SUM(CASE WHEN lead_flag_new = 'satisfaction_score' AND COALESCE(events__public, false) = false THEN 1 ELSE 0 END) AS matched_on_private_comment_new,
    SUM(CASE WHEN lead_flag_old = 'satisfaction_score' AND lead_id_old <> lead_id_new THEN 1 ELSE 0 END) AS mismatched_next_event_due_to_tie,
    SUM(CASE WHEN lead_flag_old = 'satisfaction_score' AND lead_val_old <> lead_val_new THEN 1 ELSE 0 END) AS mismatched_csat_value_due_to_tie
FROM comment_rows

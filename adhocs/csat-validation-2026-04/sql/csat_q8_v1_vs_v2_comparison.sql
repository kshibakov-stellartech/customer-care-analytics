WITH excluded_tag_patterns AS (
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
        za.ticket_id AS ticket_to_exclude_id
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
        CAST(MAX(za.events__value) AS BIGINT) AS requester_id
    FROM data_bronze_zendesk_prod.zendesk_audit za
    WHERE za.events__type = 'Create'
      AND za.events__field_name = 'requester_id'
    GROUP BY 1
    HAVING MIN(CAST(za.created_at AS DATE)) >= DATE '2026-01-01'
       AND MIN(CAST(za.created_at AS DATE)) < current_date
),
base_audit AS (
    SELECT
        za.ticket_id,
        date_add('hour', 2, za.created_at) AS created_at,
        za.events__id,
        za.events__type,
        za.events__field_name,
        za.events__value,
        za.events__public,
        CAST(CAST(za.author_id AS DOUBLE) AS BIGINT) AS author_id,
        t.requester_id
    FROM data_bronze_zendesk_prod.zendesk_audit za
    JOIN tickets t
      ON t.ticket_id = za.ticket_id
    LEFT JOIN tickets_to_exclude te
      ON te.ticket_to_exclude_id = za.ticket_id
    WHERE te.ticket_to_exclude_id IS NULL
),
scope_events AS (
    SELECT *
    FROM base_audit
    WHERE (events__type = 'Comment' AND author_id <> requester_id)
       OR (events__type = 'Change' AND events__field_name = 'satisfaction_score' AND events__value IN ('good', 'bad'))
),
score_events AS (
    SELECT
        ticket_id,
        created_at,
        events__id AS score_event_id,
        events__value AS csat_val
    FROM scope_events
    WHERE events__type = 'Change'
      AND events__field_name = 'satisfaction_score'
      AND events__value IN ('good', 'bad')
),
comment_events AS (
    SELECT
        ticket_id,
        created_at,
        events__id AS comment_event_id,
        COALESCE(events__public, false) AS comment_public
    FROM scope_events
    WHERE events__type = 'Comment'
),
v1_seq AS (
    SELECT
        se.*,
        LEAD(events__field_name) OVER (PARTITION BY ticket_id ORDER BY created_at) AS lead_flag,
        LEAD(events__id) OVER (PARTITION BY ticket_id ORDER BY created_at) AS lead_score_event_id
    FROM scope_events se
),
v1_matches AS (
    SELECT DISTINCT
        ticket_id,
        events__id AS comment_event_id,
        lead_score_event_id AS score_event_id
    FROM v1_seq
    WHERE events__type = 'Comment'
      AND lead_flag = 'satisfaction_score'
      AND lead_score_event_id IS NOT NULL
),
v2_ranked_pairs AS (
    SELECT
        s.ticket_id,
        s.score_event_id,
        s.csat_val,
        c.comment_event_id,
        c.comment_public,
        ROW_NUMBER() OVER (
            PARTITION BY s.ticket_id, s.score_event_id
            ORDER BY c.created_at DESC, c.comment_event_id DESC
        ) AS rn
    FROM score_events s
    LEFT JOIN comment_events c
      ON c.ticket_id = s.ticket_id
     AND (
            c.created_at < s.created_at
            OR (c.created_at = s.created_at AND c.comment_event_id < s.score_event_id)
         )
),
v2_matches AS (
    SELECT
        ticket_id,
        score_event_id,
        csat_val,
        comment_event_id,
        comment_public
    FROM v2_ranked_pairs
    WHERE rn = 1
      AND comment_event_id IS NOT NULL
),
v2_public_only AS (
    SELECT *
    FROM v2_matches
    WHERE comment_public = true
)
SELECT
    (SELECT COUNT(*) FROM score_events) AS total_score_events,
    (SELECT COUNT(DISTINCT ticket_id) FROM score_events) AS tickets_with_score_events,

    (SELECT COUNT(*) FROM v1_matches) AS v1_matched_score_events,
    (SELECT COUNT(*) FROM score_events s LEFT JOIN v1_matches m ON s.ticket_id = m.ticket_id AND s.score_event_id = m.score_event_id WHERE m.score_event_id IS NULL) AS v1_unmatched_score_events,
    (SELECT COUNT(*) FROM v1_matches vm JOIN comment_events ce ON ce.ticket_id = vm.ticket_id AND ce.comment_event_id = vm.comment_event_id WHERE ce.comment_public = false) AS v1_matched_on_private_comment,

    (SELECT COUNT(*) FROM v2_matches) AS v2_matched_score_events,
    (SELECT COUNT(*) FROM score_events s LEFT JOIN v2_matches m ON s.ticket_id = m.ticket_id AND s.score_event_id = m.score_event_id WHERE m.score_event_id IS NULL) AS v2_unmatched_score_events,
    (SELECT COUNT(*) FROM v2_matches WHERE comment_public = false) AS v2_matched_on_private_comment,

    (SELECT COUNT(*) FROM v2_public_only) AS v2_public_only_matched_score_events,
    (SELECT COUNT(*) FROM score_events s LEFT JOIN v2_public_only m ON s.ticket_id = m.ticket_id AND s.score_event_id = m.score_event_id WHERE m.score_event_id IS NULL) AS v2_public_only_unmatched_score_events
;

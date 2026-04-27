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
        events__id AS score_event_id
    FROM scope_events
    WHERE events__type = 'Change'
      AND events__field_name = 'satisfaction_score'
      AND events__value IN ('good', 'bad')
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
        lead_score_event_id AS score_event_id
    FROM v1_seq
    WHERE events__type = 'Comment'
      AND lead_flag = 'satisfaction_score'
      AND lead_score_event_id IS NOT NULL
),
ticket_scores AS (
    SELECT
        s.ticket_id,
        COUNT(*) AS score_cnt,
        SUM(CASE WHEN m.score_event_id IS NOT NULL THEN 1 ELSE 0 END) AS matched_cnt,
        SUM(CASE WHEN m.score_event_id IS NULL THEN 1 ELSE 0 END) AS unmatched_cnt
    FROM score_events s
    LEFT JOIN v1_matches m
      ON m.ticket_id = s.ticket_id
     AND m.score_event_id = s.score_event_id
    GROUP BY 1
),
distribution AS (
    SELECT
        CAST(score_cnt AS VARCHAR) AS bucket,
        COUNT(*) AS tickets
    FROM ticket_scores
    GROUP BY 1
),
categories AS (
    SELECT 'lost_all_scores' AS category, ticket_id, score_cnt, matched_cnt, unmatched_cnt
    FROM ticket_scores
    WHERE matched_cnt = 0
    UNION ALL
    SELECT 'perfectly_matched' AS category, ticket_id, score_cnt, matched_cnt, unmatched_cnt
    FROM ticket_scores
    WHERE unmatched_cnt = 0
    UNION ALL
    SELECT 'lost_one_kept_another' AS category, ticket_id, score_cnt, matched_cnt, unmatched_cnt
    FROM ticket_scores
    WHERE unmatched_cnt = 1
      AND matched_cnt >= 1
),
category_counts AS (
    SELECT
        category,
        COUNT(*) AS tickets
    FROM categories
    GROUP BY 1
)
SELECT
    'distribution_by_score_count' AS section,
    bucket AS metric,
    tickets,
    CAST(NULL AS VARCHAR) AS sample_ticket_ids
FROM distribution

UNION ALL

SELECT
    'category_counts' AS section,
    category AS metric,
    tickets,
    CAST(NULL AS VARCHAR) AS sample_ticket_ids
FROM category_counts

UNION ALL

SELECT
    'category_samples' AS section,
    category AS metric,
    COUNT(*) AS tickets,
    array_join(slice(array_agg(CAST(ticket_id AS VARCHAR) ORDER BY ticket_id), 1, 25), ', ') AS sample_ticket_ids
FROM categories
GROUP BY category

ORDER BY section, metric;

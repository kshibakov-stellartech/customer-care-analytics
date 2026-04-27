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
agents_dict AS (
    SELECT *
    FROM (
        VALUES
            (36064560830737), (35219779434897), (41972533108625), (40215157462161), (34224285677201),
            (26222438547857), (30648746936465), (39272670052113), (38754864964753), (38694917174545),
            (38657563018769), (38022764826129), (38022759246737), (37992873903889), (35310711957393),
            (33602186941713), (33118701264017), (33118711659921), (31467436910865), (30786139608081),
            (30655366698001), (30160506886161), (29737848444689), (26440502459665), (26349132549521),
            (42676049623057), (42676111579153), (44010183588497)
    ) AS t(agent_id)
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
        za.events__from_title,
        CAST(CAST(za.author_id AS DOUBLE) AS BIGINT) AS author_id,
        CAST(CAST(za.events__author_id AS DOUBLE) AS BIGINT) AS event_author_id,
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
            WHEN CAST(CAST(za.events__author_id AS DOUBLE) AS BIGINT) IS NOT NULL
             AND za.events__public = TRUE THEN 2
            ELSE 0
        END AS is_public_communication
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
        created_at AS score_created_at,
        events__id AS score_event_id,
        events__value AS csat_val
    FROM scope_events
    WHERE events__type = 'Change'
      AND events__field_name = 'satisfaction_score'
      AND events__value IN ('good', 'bad')
),
-- ACTUAL LOGIC (v1 immediate-next comment -> score)
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
        created_at AS comment_created_at,
        lead_score_event_id AS score_event_id
    FROM v1_seq
    WHERE events__type = 'Comment'
      AND lead_flag = 'satisfaction_score'
      AND lead_score_event_id IS NOT NULL
),
v1_latest AS (
    SELECT *
    FROM (
        SELECT
            vm.*,
            ROW_NUMBER() OVER (PARTITION BY vm.ticket_id ORDER BY vm.comment_created_at DESC, vm.comment_event_id DESC) AS rn
        FROM v1_matches vm
    ) t
    WHERE rn = 1
),
message_events AS (
    SELECT
        b.ticket_id,
        b.created_at,
        b.events__id AS event_id,
        CASE
            WHEN b.events__type = 'Notification' THEN 26440502459665
            ELSE b.author_id
        END AS author_id,
        CASE
            WHEN b.events__type = 'Comment'
             AND b.events__public = TRUE
             AND ad.agent_id IS NULL
            THEN 'customer_message'
            WHEN b.events__type = 'Comment'
             AND b.events__public = TRUE
             AND ad.agent_id IS NOT NULL
            THEN 'agent_message'
            WHEN b.events__type = 'Notification'
             AND b.is_public_communication IN (1, 2)
            THEN 'agent_message'
            ELSE 'unknown'
        END AS event_type
    FROM base_audit b
    LEFT JOIN agents_dict ad
      ON ad.agent_id = b.author_id
    WHERE
        (b.events__type = 'Comment' AND b.events__public = TRUE)
        OR (b.events__type = 'Notification' AND b.is_public_communication IN (1, 2))
),
valid_message_events AS (
    SELECT *
    FROM message_events
    WHERE event_type IN ('customer_message', 'agent_message')
),
v1_latest_joinable_to_public_message_timeline AS (
    SELECT
        l.ticket_id,
        l.score_event_id
    FROM v1_latest l
    JOIN valid_message_events vme
      ON vme.ticket_id = l.ticket_id
     AND vme.created_at = l.comment_created_at
),
-- DRAFT LOGIC (match every score to last agent message)
csat_agent_messages AS (
    SELECT
        me.ticket_id,
        me.created_at AS agent_message_created_at,
        me.event_id AS agent_message_event_id,
        me.author_id AS agent_message_author_id
    FROM valid_message_events me
    WHERE me.event_type = 'agent_message'
),
draft_ranked_prior AS (
    SELECT
        s.ticket_id,
        s.score_event_id,
        am.agent_message_created_at,
        am.agent_message_event_id,
        am.agent_message_author_id,
        ROW_NUMBER() OVER (
            PARTITION BY s.ticket_id, s.score_event_id
            ORDER BY am.agent_message_created_at DESC, am.agent_message_event_id DESC
        ) AS rn_prior
    FROM score_events s
    LEFT JOIN csat_agent_messages am
      ON am.ticket_id = s.ticket_id
     AND (
            am.agent_message_created_at < s.score_created_at
            OR (am.agent_message_created_at = s.score_created_at AND am.agent_message_event_id < s.score_event_id)
         )
),
draft_best_prior AS (
    SELECT *
    FROM draft_ranked_prior
    WHERE rn_prior = 1
),
draft_ranked_any AS (
    SELECT
        s.ticket_id,
        s.score_event_id,
        am.agent_message_created_at,
        am.agent_message_event_id,
        am.agent_message_author_id,
        ROW_NUMBER() OVER (
            PARTITION BY s.ticket_id, s.score_event_id
            ORDER BY am.agent_message_created_at DESC, am.agent_message_event_id DESC
        ) AS rn_any
    FROM score_events s
    LEFT JOIN csat_agent_messages am
      ON am.ticket_id = s.ticket_id
),
draft_best_any AS (
    SELECT *
    FROM draft_ranked_any
    WHERE rn_any = 1
),
draft_attr AS (
    SELECT
        s.ticket_id,
        s.score_event_id,
        COALESCE(p.agent_message_event_id, a.agent_message_event_id) AS matched_agent_message_event_id,
        CASE
            WHEN p.agent_message_event_id IS NOT NULL THEN 'last_agent_message_before_score'
            WHEN a.agent_message_event_id IS NOT NULL THEN 'fallback_last_agent_message_in_ticket'
            ELSE 'unmatched_no_agent_message_in_ticket'
        END AS draft_match_rule
    FROM score_events s
    LEFT JOIN draft_best_prior p
      ON p.ticket_id = s.ticket_id
     AND p.score_event_id = s.score_event_id
    LEFT JOIN draft_best_any a
      ON a.ticket_id = s.ticket_id
     AND a.score_event_id = s.score_event_id
)
SELECT
    (SELECT COUNT(*) FROM score_events) AS total_score_events,
    (SELECT COUNT(DISTINCT ticket_id) FROM score_events) AS tickets_with_score,

    (SELECT COUNT(*) FROM v1_matches) AS actual_v1_matched_score_events,
    (SELECT COUNT(*) FROM score_events s LEFT JOIN v1_matches m ON s.ticket_id = m.ticket_id AND s.score_event_id = m.score_event_id WHERE m.score_event_id IS NULL) AS actual_v1_unmatched_score_events,
    (SELECT COUNT(*) FROM v1_latest) AS actual_latest_rows,
    (SELECT COUNT(*) FROM v1_latest_joinable_to_public_message_timeline) AS actual_latest_rows_joinable_to_message_timeline,

    (SELECT COUNT(*) FROM draft_attr WHERE matched_agent_message_event_id IS NOT NULL) AS draft_matched_score_events_total,
    (SELECT COUNT(*) FROM draft_attr WHERE draft_match_rule = 'last_agent_message_before_score') AS draft_matched_by_prior_rule,
    (SELECT COUNT(*) FROM draft_attr WHERE draft_match_rule = 'fallback_last_agent_message_in_ticket') AS draft_matched_by_fallback_rule,
    (SELECT COUNT(*) FROM draft_attr WHERE draft_match_rule = 'unmatched_no_agent_message_in_ticket') AS draft_unmatched_no_agent_message,

    (SELECT COUNT(*)
     FROM draft_attr d
     JOIN v1_matches v
       ON v.ticket_id = d.ticket_id
      AND v.score_event_id = d.score_event_id
     WHERE d.matched_agent_message_event_id IS NOT NULL
       AND d.matched_agent_message_event_id <> v.comment_event_id) AS mapping_changed_vs_actual
;

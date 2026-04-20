WITH
/* =========================================================
0. reference dictionaries
========================================================= */
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
auto_reply_titles AS (
    SELECT *
    FROM (
        VALUES
            ('Auto_12: Auto-reply to refund requests (Stores)'),
            ('Auto_21: Auto-reply to delete+refund requests (Paddle/PayPal)'),
            ('Auto_91: Auto-reply to delete requests (Stores)'),
            ('Auto_13: Auto-reply to refund requests (Paddle/PayPal)'),
            ('Auto_29: Auto-reply - payment_not_found AI'),
            ('Auto_29: Auto-reply - payment_not_found AI (2nd)'),
            ('Auto_29: Auto-reply - payment_not_found (automation failed)'),
            ('Auto_35: Auto-reply to delete+refund requests (threats/risk)'),
            ('Auto_6: Auto-reply to cancel requests (Web) '),
            ('Auto_7: Auto-reply to cancel requests (Stores)'),
            ('Auto_28: Freemium only - payment_not_found'),
            ('Auto-reply - something is wrong with my subscription - SmartyMe')
    ) AS t(from_title)
),
agents_dict AS (
    SELECT *
    FROM (
        VALUES
            (36064560830737, 'Mykyta', 'Admins'),
            (35219779434897, 'Ilia Tregubov', 'Admins'),
            (41972533108625, 'Konstantin Shibakov', 'Admins'),
            (40215157462161, 'QA', 'Admins'),
            (34224285677201, 'Yaroslav Kukharenko', 'Admins'),
            (26222438547857, 'Maksym Zvieriev', 'TL'),
            (30648746936465, 'Alexander Petrov', 'TL'),
            (39272670052113, 'Sam Bondar', 'Moon Rangers'),
            (38754864964753, 'Brian Tepliuk', 'Moon Rangers'),
            (38694917174545, 'Mike Mkrtumyan', 'Moon Rangers'),
            (38657563018769, 'Alice Sakharova', 'Moon Rangers'),
            (38022764826129, 'Allie Kostukovich', 'Blanc'),
            (38022759246737, 'Kate Rumiantseva', 'Moon Rangers'),
            (37992873903889, 'Ann Dereka', 'Moon Rangers'),
            (35310711957393, 'Anette Monaselidze', 'Blanc'),
            (33602186941713, 'Jackie Si', 'Blanc'),
            (33118701264017, 'Daria Saranchova', 'Blanc'),
            (33118711659921, 'Katrina Novikova', 'Blanc'),
            (31467436910865, 'Jenny', 'Moon Rangers'),
            (30786139608081, 'Jade Kasper', 'Blanc'),
            (30655366698001, 'Catherine Moroz', 'Blanc'),
            (30160506886161, 'Alex Poponin', 'Blanc'),
            (29737848444689, 'Daniel Vinokurov', 'Blanc'),
            (26440502459665, 'Nikki', 'Automation'),
            (26349132549521, 'Mia Petchenko', 'Moon Rangers'),
            (42676049623057, 'Sophie Palamarchuk', 'Moon Rangers'),
            (42676111579153, 'Michael Brodovskyi', 'Moon Rangers'),
            (44010183588497, 'Stella Kishyk', 'Blanc')
    ) AS t(agent_id, agent_name, agent_group)
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
    GROUP BY 1
    HAVING MIN(CAST(za.created_at AS DATE)) >= DATE '2026-01-01'
       AND MIN(CAST(za.created_at AS DATE)) < current_date
),

base_audit AS (
    SELECT
        za.ticket_id,
        t.ticket_created_at,
        t.requester_id,
        za.channel,
        date_add('hour', 2, za.created_at) AS created_at,
        date_trunc('minute', date_add('hour', 2, za.created_at)) AS created_at_truncated,
        CAST(CAST(za.author_id AS DOUBLE) AS BIGINT) AS author_id,
        CAST(CAST(za.events__author_id AS DOUBLE) AS BIGINT) AS event_author_id,
        za.events__id,
        za.events__type,
        za.events__field_name,
        za.events__value,
        TRY_CAST(za.events__value AS BIGINT) AS events__value_bigint,
        za.events__previous_value,
        za.events__body,
        za.events__public,
        za.events__from_title,
        CASE
            WHEN za.events__type = 'Notification'
             AND EXISTS (
                SELECT 1
                FROM auto_reply_titles art
                WHERE art.from_title = za.events__from_title
             )
            THEN 1
            WHEN CAST(CAST(za.events__author_id AS DOUBLE) AS BIGINT) IS NOT NULL
             AND za.events__public = TRUE
            THEN 2
            ELSE 0
        END AS is_public_communication
    FROM data_bronze_zendesk_prod.zendesk_audit za
    JOIN tickets t
      ON t.ticket_id = za.ticket_id
    LEFT JOIN tickets_to_exclude te
      ON te.ticket_to_exclude_id = za.ticket_id
    WHERE te.ticket_to_exclude_id IS NULL
      -- AND za.ticket_id = 762083
),

tickets_attr AS (
    SELECT
        b.ticket_id,
        b.ticket_created_at,
        CAST(CAST(b.requester_id AS DOUBLE) AS BIGINT) AS requester_id,
        MAX(
            CASE
                WHEN events__field_name IN ('32351109113361', '40831328206865', '32351085497873')
                THEN events__value
            END
        ) AS user_id,
        MAX(
            CASE
                WHEN events__type = 'Create' AND events__field_name = 'brand_id' THEN
                    CASE
                        WHEN events__value = '26467992035601' THEN 'MindScape'
                        WHEN events__value = '27810244289553' THEN 'Neurolift'
                        WHEN events__value = '26468032413713' THEN 'SmartyMe'
                        WHEN events__value = '26222456156689' THEN 'StellarTech Limited'
                        WHEN events__value = '43023476289553' THEN 'Nexera'
                        ELSE 'Unknown'
                    END
            END
        ) AS ticket_brand,
        MAX(
            CASE
                WHEN events__type = 'Create' AND events__field_name = 'ticket_form_id' THEN
                    CASE
                        WHEN events__value = '26472204214801' THEN 'main ticket form'
                        WHEN events__value = '34833592831505' THEN 'in-app ticket form'
                        WHEN events__value = '34902185196177' THEN 'test form'
                        WHEN events__value = '26222488220945' THEN 'default ticket form'
                        WHEN events__value = '35743604923281' THEN 'registration form'
                        ELSE NULL
                    END
            END
        ) AS ticket_form_type,
        MAX(CASE WHEN events__type = 'Create' AND events__field_name = 'requester_id' THEN channel END) AS ticket_channel,
        MAX(CASE WHEN events__type = 'Create' AND events__field_name = 'subject' THEN events__value END) AS ticket_subject,
        ELEMENT_AT(
            ARRAY_AGG(
                CASE
                    WHEN events__value = '26222456191633' THEN 'new'
                    WHEN events__value = '26222456196881' THEN 'open'
                    WHEN events__value = '26222488415249' THEN 'in progress'
                    WHEN events__value = '26471687892881' THEN 'waiting for cs'
                    WHEN events__value = '26222456200081' THEN 'pending'
                    WHEN events__value = '26471092160657' THEN 'waiting for customer'
                    WHEN events__value = '26471128667153' THEN 'waiting for tech team'
                    WHEN events__value = '26222456206737' THEN 'solved'
                    WHEN events__value = '26471115820177' THEN 'solved (no reply)'
                    WHEN events__value = 'closed' THEN 'closed'
                    ELSE NULL
                END
                ORDER BY created_at DESC, events__id DESC
            ) FILTER (
                WHERE
                    (events__field_name = 'custom_status_id' AND events__value IS NOT NULL)
                    OR (events__field_name = 'status' AND events__value = 'closed')
            ),
            1
        ) AS status,
        ELEMENT_AT(
            ARRAY_AGG(events__value ORDER BY created_at DESC, events__id DESC)
            FILTER (WHERE events__field_name = '26442658996241' AND events__value IS NOT NULL),
            1
        ) AS request_type,
        ELEMENT_AT(
            ARRAY_AGG(events__value ORDER BY created_at DESC, events__id DESC)
            FILTER (WHERE events__field_name = '31320582354705' AND events__value IS NOT NULL),
            1
        ) AS subtype,
       MAX(CASE WHEN events__type = 'SurveyOffered' THEN 1  ELSE 0 END) as survey_offered,
       MAX(CASE WHEN events__type = 'SurveyResponseSubmitted' THEN 1  ELSE 0 END) as survey_submitted,
       MAX(CASE WHEN events__field_name = 'tags' AND events__value LIKE '%refund%'     THEN 1 ELSE 0 END) as refund_tag,
       MAX(CASE WHEN events__field_name = 'tags' AND events__value LIKE '%refund_not_eligible%' THEN 1 ELSE 0 END) as refund_not_eligible,
       MAX(CASE WHEN events__field_name = 'tags' AND events__value LIKE '%refund_eligible%'     THEN 1 ELSE 0 END) as refund_eligible,
       MAX(CASE WHEN events__field_name = 'assignee_id' AND TRY_CAST(events__value AS BIGINT) = 26440502459665 THEN 1  ELSE 0 END) as auto_involved,
       CASE WHEN ELEMENT_AT(
           ARRAY_AGG(author_id ORDER BY created_at DESC, events__id DESC)
           FILTER (WHERE events__type = 'Comment' AND events__public = true), 1
       ) = 26440502459665 THEN 1 ELSE 0 END as auto_resolved,
       MAX(CASE WHEN events__field_name = 'tags' AND events__value LIKE '%tech_team%' THEN 1 ELSE 0 END) as tech_team_involved,
       MAX(DATE_DIFF('second', ticket_created_at, CASE WHEN events__field_name = 'custom_status_id' AND events__value IN ('26222456206737', '26471115820177') THEN created_at
                                                       WHEN events__type = 'Comment' AND author_id <> requester_id THEN created_at
                                                  END
                    )
       ) as resolution_time
    FROM base_audit b
    GROUP BY 1, 2, 3
),

csat_attr AS (
    SELECT
        ticket_id,
        created_at,
        events__id,
        author_id AS csat_author_id,
        csat_val,
        ROW_NUMBER() OVER (PARTITION BY ticket_id ORDER BY created_at DESC) AS csat_rn
    FROM (
        SELECT
            ticket_id,
            created_at,
            author_id,
            events__id,
            events__type,
            events__field_name,
            events__value,
            LEAD(events__field_name) OVER (PARTITION BY ticket_id ORDER BY created_at) AS csat_flag,
            LEAD(events__value) OVER (PARTITION BY ticket_id ORDER BY created_at) AS csat_val
        FROM base_audit
        WHERE
            (events__type = 'Comment' AND author_id <> requester_id)
            OR (events__type = 'Change' AND events__field_name = 'satisfaction_score' AND events__value IN ('good', 'bad'))
    ) raw_csat
    WHERE events__type = 'Comment'
      AND csat_flag = 'satisfaction_score'
),
tech_team AS (
  --tech_team_time, подзапрос для расчетов
SELECT ticket_id, SUM(tech_team_time) as tech_team_duration_sec
FROM (
  SELECT ticket_id,
         created_at,
         events__value,
         DATE_DIFF('second', created_at, LEAD(created_at) OVER(PARTITION BY ticket_id ORDER BY created_at)) as tech_team_time
  FROM base_audit
  WHERE 1=1
    AND events__field_name = 'custom_status_id'
    AND events__value IN (
        '26471128667153', /* waiting for tech team */
        '26222456206737', /* solved */
        '26471115820177'  /* (no reply) */
                         )
ORDER BY ticket_id, created_at
) raw_tech
WHERE 1=1
  AND events__value = '26471128667153'
GROUP BY 1
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
        b.requester_id,
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
        END AS event_type,
        b.events__body AS msg_text,
        b.events__type,
        b.events__public,
        b.events__from_title
    FROM base_audit b
    LEFT JOIN agents_dict ad
      ON ad.agent_id = b.author_id
    WHERE
        (
            b.events__type = 'Comment'
            AND b.events__public = TRUE
        )
        OR
        (
            b.events__type = 'Notification'
            AND b.is_public_communication IN (1, 2)
        )
),

valid_message_events AS (
    SELECT *
    FROM message_events
    WHERE event_type IN ('customer_message', 'agent_message')
),

solved_events AS (
    SELECT
        b.ticket_id,
        b.created_at,
        b.events__id AS event_id,
        b.author_id,
        CASE
            WHEN b.events__field_name = 'custom_status_id'
             AND b.events__value = '26222456206737'
            THEN 'solved'
            WHEN b.events__field_name = 'custom_status_id'
             AND b.events__value = '26471115820177'
            THEN 'solved_no_reply'
        END AS solved_type
    FROM base_audit b
    WHERE b.events__field_name = 'custom_status_id'
      AND b.events__value IN ('26222456206737', '26471115820177')
),

segmentation_events AS (
    SELECT
        me.ticket_id,
        me.created_at,
        me.event_id,
        me.author_id,
        me.requester_id,
        me.event_type,
        me.msg_text
    FROM valid_message_events me
    UNION ALL
    SELECT
        se.ticket_id,
        se.created_at,
        se.event_id,
        se.author_id,
        CAST(NULL AS BIGINT) AS requester_id,
        'solved_event' AS event_type,
        CAST(NULL AS VARCHAR) AS msg_text
    FROM solved_events se
),

segmentation_ordered AS (
    SELECT
        se.*,
        CASE
            WHEN se.event_type = 'customer_message' THEN 1
            WHEN se.event_type = 'solved_event' THEN 2
            WHEN se.event_type = 'agent_message' THEN 3
        END AS seg_priority,
        LAG(se.event_type) OVER (
            PARTITION BY se.ticket_id
            ORDER BY se.created_at,
                     CASE
                         WHEN se.event_type = 'customer_message' THEN 1
                         WHEN se.event_type = 'solved_event' THEN 2
                         WHEN se.event_type = 'agent_message' THEN 3
                     END,
                     se.event_id
        ) AS prev_seg_event_type
    FROM segmentation_events se
),

segmentation_blocked AS (
    SELECT
        so.*,
        CASE
            WHEN so.event_type = 'customer_message'
             AND COALESCE(so.prev_seg_event_type, 'closure') IN ('agent_message', 'solved_event', 'closure')
            THEN 1
            ELSE 0
        END AS is_new_customer_block
    FROM segmentation_ordered so
),

segmentation_numbered AS (
    SELECT
        sb.*,
        SUM(sb.is_new_customer_block) OVER (
            PARTITION BY sb.ticket_id
            ORDER BY sb.created_at, sb.seg_priority, sb.event_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS customer_msg_num
    FROM segmentation_blocked sb
),

customer_blocks AS (
    SELECT
        ticket_id,
        customer_msg_num,
        MIN(created_at) AS customer_block_start_at
    FROM segmentation_numbered
    WHERE event_type = 'customer_message'
    GROUP BY 1,2
),

message_enriched AS (
    SELECT
        me.*,
        sn.customer_msg_num,
        cb.customer_block_start_at,
        LAG(me.event_type) OVER (
            PARTITION BY me.ticket_id
            ORDER BY me.created_at, me.event_id
        ) AS prev_public_message_type
    FROM valid_message_events me
    LEFT JOIN segmentation_numbered sn
      ON sn.ticket_id = me.ticket_id
     AND sn.event_id = me.event_id
     AND sn.event_type = me.event_type
    LEFT JOIN customer_blocks cb
      ON cb.ticket_id = sn.ticket_id
     AND cb.customer_msg_num = sn.customer_msg_num
),

reply_candidates AS (
    SELECT
        me.*,
        CASE
            WHEN me.event_type = 'agent_message'
             AND me.customer_msg_num > 0
             AND COALESCE(me.prev_public_message_type, 'none') = 'customer_message'
            THEN 1
            ELSE 0
        END AS is_first_agent_reply
    FROM message_enriched me
),

reply_numbered AS (
    SELECT
        rc.*,
        CASE
            WHEN rc.is_first_agent_reply = 1 THEN
                SUM(rc.is_first_agent_reply) OVER (
                    PARTITION BY rc.ticket_id
                    ORDER BY rc.created_at, rc.event_id
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )
        END AS agent_reply_num
    FROM reply_candidates rc
),

first_reply_per_block AS (
    SELECT
        ticket_id,
        customer_msg_num,
        MIN(created_at) AS first_reply_at
    FROM reply_numbered
    WHERE is_first_agent_reply = 1
    GROUP BY 1, 2
),

first_solved_per_block AS (
    SELECT
        sn.ticket_id,
        sn.customer_msg_num,
        MIN(sn.created_at) AS first_solved_at
    FROM segmentation_numbered sn
    WHERE sn.event_type = 'solved_event'
    GROUP BY 1,2
),

block_closure AS (
    SELECT
        cb.ticket_id,
        cb.customer_msg_num,
        cb.customer_block_start_at,
        fr.first_reply_at,
        fs.first_solved_at,
        CASE
            WHEN fr.first_reply_at IS NOT NULL
             AND (fs.first_solved_at IS NULL OR fr.first_reply_at <= fs.first_solved_at)
            THEN fr.first_reply_at
            WHEN fs.first_solved_at IS NOT NULL
            THEN fs.first_solved_at
            ELSE NULL
        END AS block_closed_at,
        CASE
            WHEN fr.first_reply_at IS NOT NULL
             AND (fs.first_solved_at IS NULL OR fr.first_reply_at <= fs.first_solved_at)
            THEN 'reply'
            WHEN fs.first_solved_at IS NOT NULL
            THEN 'solved_without_reply'
            ELSE NULL
        END AS block_closure_type
    FROM customer_blocks cb
    LEFT JOIN first_reply_per_block fr
      ON fr.ticket_id = cb.ticket_id
     AND fr.customer_msg_num = cb.customer_msg_num
    LEFT JOIN first_solved_per_block fs
      ON fs.ticket_id = cb.ticket_id
     AND fs.customer_msg_num = cb.customer_msg_num
),

customer_messages_ranked AS (
    SELECT
        rn.ticket_id,
        rn.event_id,
        rn.author_id,
        rn.created_at,
        rn.msg_text,
        rn.customer_msg_num,
        rn.customer_block_start_at,
        ROW_NUMBER() OVER (
            PARTITION BY rn.ticket_id, rn.customer_msg_num
            ORDER BY rn.created_at, rn.event_id
        ) AS rn_in_block,
        LAG(rn.created_at) OVER (
            PARTITION BY rn.ticket_id, rn.customer_msg_num
            ORDER BY rn.created_at, rn.event_id
        ) AS prev_customer_msg_at
    FROM reply_numbered rn
    WHERE rn.event_type = 'customer_message'
),

customer_block_prev_reply AS (
    SELECT
        cb.ticket_id,
        cb.customer_msg_num,
        MAX(r.created_at) AS previous_agent_reply_at
    FROM customer_blocks cb
    LEFT JOIN reply_numbered r
      ON r.ticket_id = cb.ticket_id
     AND r.is_first_agent_reply = 1
     AND r.created_at < cb.customer_block_start_at
    GROUP BY 1, 2
),

customer_message_log AS (
    SELECT
        cmr.created_at AS created_at,
        cmr.ticket_id,
        'customer_message' AS log_type,
        cmr.author_id,
        cmr.created_at AS msg_created_at,
        CAST(NULL AS TIMESTAMP) AS assign_created_at,
        CASE
            WHEN cmr.rn_in_block = 1
                THEN COALESCE(cbpr.previous_agent_reply_at, cmr.created_at)
            ELSE cmr.prev_customer_msg_at
        END AS previous_event_at,
        bc.block_closed_at AS next_event_at,
        CASE
            WHEN cmr.rn_in_block = 1 THEN
                CASE
                    WHEN COALESCE(cbpr.previous_agent_reply_at, cmr.created_at) = cmr.created_at THEN 0
                    ELSE DATE_DIFF('second', COALESCE(cbpr.previous_agent_reply_at, cmr.created_at), cmr.created_at)
                END
            ELSE DATE_DIFF('second', cmr.prev_customer_msg_at, cmr.created_at)
        END AS duration_sec_overall,
        CAST(NULL AS BIGINT) AS duration_sec_agent,
        cmr.customer_msg_num,
        CAST(NULL AS BIGINT) AS agent_reply_num,
        cmr.msg_text,
        CASE WHEN bc.block_closure_type = 'solved_without_reply' THEN 1 ELSE 0 END AS closed_without_reply_flag
    FROM customer_messages_ranked cmr
    LEFT JOIN customer_block_prev_reply cbpr
      ON cbpr.ticket_id = cmr.ticket_id
     AND cbpr.customer_msg_num = cmr.customer_msg_num
    LEFT JOIN block_closure bc
      ON bc.ticket_id = cmr.ticket_id
     AND bc.customer_msg_num = cmr.customer_msg_num
),

assignment_events AS (
    SELECT
        b.ticket_id,
        b.created_at,
        b.events__id AS event_id,
        b.author_id AS assignment_author_id,
        b.requester_id,
        b.events__value_bigint AS assigned_agent_id
    FROM base_audit b
    WHERE b.events__field_name = 'assignee_id'
      AND b.events__value_bigint IS NOT NULL
),

assignment_next_assignment AS (
    SELECT
        ae.*,
        LEAD(ae.created_at) OVER (
            PARTITION BY ae.ticket_id
            ORDER BY ae.created_at, ae.event_id
        ) AS next_assignment_at
    FROM assignment_events ae
),

assignment_first_public_event AS (
    SELECT
        ana.ticket_id,
        ana.event_id,
        ana.assignment_author_id,
        ana.assigned_agent_id,
        ana.created_at AS assign_created_at,
        ana.next_assignment_at,
        MIN(me.created_at) AS first_public_after_assign_at
    FROM assignment_next_assignment ana
    LEFT JOIN valid_message_events me
      ON me.ticket_id = ana.ticket_id
     AND (
            me.created_at > ana.created_at
            OR (me.created_at = ana.created_at AND me.event_type = 'agent_message')
         )
     AND (
            ana.next_assignment_at IS NULL
            OR me.created_at < ana.next_assignment_at
         )
    GROUP BY 1,2,3,4,5,6
),

assignment_first_public_candidates AS (
    SELECT
        afpe.ticket_id,
        afpe.event_id,
        afpe.assignment_author_id,
        afpe.assigned_agent_id,
        afpe.assign_created_at,
        afpe.next_assignment_at,
        afpe.first_public_after_assign_at,
        me.event_type AS first_public_event_type,
        me.author_id AS first_public_author_id,
        ROW_NUMBER() OVER (
            PARTITION BY afpe.ticket_id, afpe.event_id
            ORDER BY me.created_at, me.event_id
        ) AS rn
    FROM assignment_first_public_event afpe
    LEFT JOIN valid_message_events me
      ON me.ticket_id = afpe.ticket_id
     AND me.created_at = afpe.first_public_after_assign_at
),

assignment_intervals AS (
    SELECT
        afpc.ticket_id,
        afpc.event_id,
        afpc.assignment_author_id,
        afpc.assigned_agent_id,
        afpc.assign_created_at,
        afpc.next_assignment_at,
        afpc.first_public_after_assign_at,
        afpc.first_public_event_type,
        afpc.first_public_author_id,
        CASE
            WHEN afpc.first_public_event_type = 'agent_message'
             AND afpc.first_public_author_id = afpc.assigned_agent_id
            THEN 1
            ELSE 0
        END AS assignment_consumed_by_reply,
        COALESCE(afpc.first_public_after_assign_at, afpc.next_assignment_at) AS assignment_end_at
    FROM assignment_first_public_candidates afpc
    WHERE afpc.rn = 1
),

all_events AS (
    SELECT
        rn.ticket_id,
        rn.created_at,
        rn.event_id,
        rn.author_id AS actor_id,
        rn.requester_id,
        CAST(NULL AS BIGINT) AS assigned_agent_id,
        CAST(NULL AS BIGINT) AS assignment_author_id,
        rn.event_type,
        rn.msg_text,
        CASE
            WHEN rn.event_type = 'customer_message' THEN 1
            WHEN rn.event_type = 'agent_message' THEN 3
        END AS event_priority
    FROM reply_numbered rn
    UNION ALL
    SELECT
        ae.ticket_id,
        ae.created_at,
        ae.event_id,
        ae.assignment_author_id AS actor_id,
        ae.requester_id,
        ae.assigned_agent_id,
        ae.assignment_author_id,
        'assignment' AS event_type,
        CAST(NULL AS VARCHAR) AS msg_text,
        2 AS event_priority
    FROM assignment_events ae
    UNION ALL
    SELECT
        se.ticket_id,
        se.created_at,
        se.event_id,
        se.author_id AS actor_id,
        CAST(NULL AS BIGINT) AS requester_id,
        CAST(NULL AS BIGINT) AS assigned_agent_id,
        CAST(NULL AS BIGINT) AS assignment_author_id,
        'solved_event' AS event_type,
        CAST(NULL AS VARCHAR) AS msg_text,
        2 AS event_priority
    FROM solved_events se
),

ordered_all_events AS (
    SELECT
        ae.*,
        LEAD(ae.created_at) OVER (
            PARTITION BY ae.ticket_id
            ORDER BY ae.created_at, ae.event_priority, ae.event_id
        ) AS next_event_at_any
    FROM all_events ae
),

first_replies AS (
    SELECT
        rn.ticket_id,
        rn.event_id,
        rn.author_id,
        rn.created_at AS reply_created_at,
        rn.msg_text,
        rn.customer_msg_num,
        rn.agent_reply_num,
        rn.customer_block_start_at,
        oae.next_event_at_any AS next_event_at
    FROM reply_numbered rn
    JOIN ordered_all_events oae
      ON oae.ticket_id = rn.ticket_id
     AND oae.event_id = rn.event_id
     AND oae.event_type = rn.event_type
    WHERE rn.is_first_agent_reply = 1
),

reply_assignment_match AS (
    SELECT
        fr.ticket_id,
        fr.event_id,
        fr.author_id,
        fr.reply_created_at,
        fr.msg_text,
        fr.customer_msg_num,
        fr.agent_reply_num,
        fr.customer_block_start_at,
        fr.next_event_at,
        ai.assign_created_at,
        ROW_NUMBER() OVER (
            PARTITION BY fr.ticket_id, fr.event_id
            ORDER BY ai.assign_created_at DESC, ai.event_id DESC
        ) AS rn_desc
    FROM first_replies fr
    LEFT JOIN assignment_intervals ai
      ON ai.ticket_id = fr.ticket_id
     AND ai.assigned_agent_id = fr.author_id
     AND ai.assignment_consumed_by_reply = 1
     AND ai.first_public_after_assign_at = fr.reply_created_at
),

reply_with_assignment_status AS (
    SELECT
        ram.ticket_id,
        ram.event_id,
        ram.author_id,
        ram.reply_created_at,
        ram.msg_text,
        ram.customer_msg_num,
        ram.agent_reply_num,
        ram.customer_block_start_at,
        ram.next_event_at,
        ram.assign_created_at,
        CASE WHEN ram.assign_created_at IS NOT NULL THEN 1 ELSE 0 END AS has_valid_assignment
    FROM reply_assignment_match ram
    WHERE ram.rn_desc = 1
),

last_any_assignment_before_reply AS (
    SELECT
        fr.ticket_id,
        fr.event_id,
        MAX(ae.created_at) AS last_any_assign_at
    FROM first_replies fr
    LEFT JOIN assignment_events ae
      ON ae.ticket_id = fr.ticket_id
     AND ae.created_at <= fr.reply_created_at
    GROUP BY 1,2
),

reply_by_assignment_log AS (
    SELECT
        rwas.reply_created_at AS created_at,
        rwas.ticket_id,
        'reply_by_assignment_after_customer_msg' AS log_type,
        rwas.author_id,
        rwas.reply_created_at AS msg_created_at,
        rwas.assign_created_at,
        rwas.customer_block_start_at AS previous_event_at,
        rwas.next_event_at,
        DATE_DIFF('second', rwas.customer_block_start_at, rwas.reply_created_at) AS duration_sec_overall,
        DATE_DIFF('second', rwas.assign_created_at, rwas.reply_created_at) AS duration_sec_agent,
        rwas.customer_msg_num,
        rwas.agent_reply_num,
        rwas.msg_text,
        0 AS closed_without_reply_flag
    FROM reply_with_assignment_status rwas
    WHERE rwas.has_valid_assignment = 1
),

reply_without_assignment_log AS (
    SELECT
        rwas.reply_created_at AS created_at,
        rwas.ticket_id,
        'reply_without_assignment' AS log_type,
        rwas.author_id,
        rwas.reply_created_at AS msg_created_at,
        CAST(NULL AS TIMESTAMP) AS assign_created_at,
        CASE
            WHEN rwas.customer_block_start_at IS NOT NULL THEN rwas.customer_block_start_at
            ELSE laabr.last_any_assign_at
        END AS previous_event_at,
        rwas.next_event_at,
        DATE_DIFF('second', rwas.customer_block_start_at, rwas.reply_created_at) AS duration_sec_overall,
        0 AS duration_sec_agent,
        rwas.customer_msg_num,
        rwas.agent_reply_num,
        rwas.msg_text,
        0 AS closed_without_reply_flag
    FROM reply_with_assignment_status rwas
    LEFT JOIN last_any_assignment_before_reply laabr
      ON laabr.ticket_id = rwas.ticket_id
     AND laabr.event_id = rwas.event_id
    WHERE rwas.has_valid_assignment = 0
),

solved_without_reply_log AS (
    SELECT
        bc.first_solved_at AS created_at,
        bc.ticket_id,
        'solved_without_reply' AS log_type,
        CAST(NULL AS BIGINT) AS author_id,
        CAST(NULL AS TIMESTAMP) AS msg_created_at,
        CAST(NULL AS TIMESTAMP) AS assign_created_at,
        bc.customer_block_start_at AS previous_event_at,
        oae.next_event_at_any AS next_event_at,
        CAST(NULL AS BIGINT) AS duration_sec_overall,
        CAST(NULL AS BIGINT) AS duration_sec_agent,
        bc.customer_msg_num,
        CAST(NULL AS BIGINT) AS agent_reply_num,
        CAST(NULL AS VARCHAR) AS msg_text,
        1 AS closed_without_reply_flag
    FROM block_closure bc
    LEFT JOIN ordered_all_events oae
      ON oae.ticket_id = bc.ticket_id
     AND oae.created_at = bc.first_solved_at
     AND oae.event_type = 'solved_event'
    WHERE bc.block_closure_type = 'solved_without_reply'
),

agent_message_log AS (
    SELECT
        rn.created_at AS created_at,
        rn.ticket_id,
        'agent_message' AS log_type,
        rn.author_id,
        rn.created_at AS msg_created_at,
        CAST(NULL AS TIMESTAMP) AS assign_created_at,
        CAST(NULL AS TIMESTAMP) AS previous_event_at,
        oae.next_event_at_any AS next_event_at,
        CAST(NULL AS BIGINT) AS duration_sec_overall,
        CAST(NULL AS BIGINT) AS duration_sec_agent,
        rn.customer_msg_num,
        CAST(NULL AS BIGINT) AS agent_reply_num,
        rn.msg_text,
        0 AS closed_without_reply_flag
    FROM reply_numbered rn
    JOIN ordered_all_events oae
      ON oae.ticket_id = rn.ticket_id
     AND oae.event_id = rn.event_id
     AND oae.event_type = rn.event_type
    WHERE rn.event_type = 'agent_message'
      AND COALESCE(rn.is_first_agent_reply, 0) = 0
),

assignment_without_reply_raw AS (
    SELECT
        ai.ticket_id,
        ai.event_id,
        ai.assigned_agent_id AS author_id,
        ai.assign_created_at,
        ai.assignment_end_at AS next_event_at
    FROM assignment_intervals ai
    WHERE ai.assignment_consumed_by_reply = 0
      AND ai.assignment_end_at IS NOT NULL
),

last_any_assignment_before_assignment AS (
    SELECT
        cur.ticket_id,
        cur.event_id,
        MAX(prev.assign_created_at) AS last_prev_assign_at
    FROM assignment_without_reply_raw cur
    LEFT JOIN assignment_without_reply_raw prev
      ON prev.ticket_id = cur.ticket_id
     AND prev.assign_created_at < cur.assign_created_at
    GROUP BY 1,2
),

customer_anchor_for_assignment AS (
    SELECT
        awr.ticket_id,
        awr.event_id,
        MAX_BY(cb.customer_block_start_at, cb.customer_block_start_at) AS customer_block_start_at,
        MAX_BY(cb.customer_msg_num, cb.customer_block_start_at) AS customer_msg_num
    FROM assignment_without_reply_raw awr
    LEFT JOIN customer_blocks cb
      ON cb.ticket_id = awr.ticket_id
     AND cb.customer_block_start_at <= awr.assign_created_at
    GROUP BY 1,2
),

assignment_without_reply_log AS (
    SELECT
        awr.assign_created_at AS created_at,
        awr.ticket_id,
        'assignment_without_reply' AS log_type,
        awr.author_id,
        CAST(NULL AS TIMESTAMP) AS msg_created_at,
        awr.assign_created_at,
        CASE
            WHEN ca.customer_block_start_at IS NOT NULL THEN ca.customer_block_start_at
            ELSE laba.last_prev_assign_at
        END AS previous_event_at,
        awr.next_event_at,
        DATE_DIFF('second', awr.assign_created_at, awr.next_event_at) AS duration_sec_overall,
        CAST(NULL AS BIGINT) AS duration_sec_agent,
        ca.customer_msg_num,
        CAST(NULL AS BIGINT) AS agent_reply_num,
        CAST(NULL AS VARCHAR) AS msg_text,
        0 AS closed_without_reply_flag
    FROM assignment_without_reply_raw awr
    LEFT JOIN last_any_assignment_before_assignment laba
      ON laba.ticket_id = awr.ticket_id
     AND laba.event_id = awr.event_id
    LEFT JOIN customer_anchor_for_assignment ca
      ON ca.ticket_id = awr.ticket_id
     AND ca.event_id = awr.event_id
),

final_log AS (
    SELECT
        created_at,
        ticket_id,
        log_type,
        author_id,
        msg_created_at,
        assign_created_at,
        previous_event_at,
        next_event_at,
        duration_sec_overall,
        duration_sec_agent,
        customer_msg_num,
        agent_reply_num,
        msg_text,
        closed_without_reply_flag
    FROM customer_message_log
    UNION ALL
    SELECT
        created_at, ticket_id, log_type, author_id, msg_created_at, assign_created_at,
        previous_event_at, next_event_at, duration_sec_overall, duration_sec_agent,
        customer_msg_num, agent_reply_num, msg_text, closed_without_reply_flag
    FROM reply_by_assignment_log
    UNION ALL
    SELECT
        created_at, ticket_id, log_type, author_id, msg_created_at, assign_created_at,
        previous_event_at, next_event_at, duration_sec_overall, duration_sec_agent,
        customer_msg_num, agent_reply_num, msg_text, closed_without_reply_flag
    FROM reply_without_assignment_log
    UNION ALL
    SELECT
        created_at, ticket_id, log_type, author_id, msg_created_at, assign_created_at,
        previous_event_at, next_event_at, duration_sec_overall, duration_sec_agent,
        customer_msg_num, agent_reply_num, msg_text, closed_without_reply_flag
    FROM solved_without_reply_log
    UNION ALL
    SELECT
        created_at, ticket_id, log_type, author_id, msg_created_at, assign_created_at,
        previous_event_at, next_event_at, duration_sec_overall, duration_sec_agent,
        customer_msg_num, agent_reply_num, msg_text, closed_without_reply_flag
    FROM assignment_without_reply_log
    UNION ALL
    SELECT
        created_at, ticket_id, log_type, author_id, msg_created_at, assign_created_at,
        previous_event_at, next_event_at, duration_sec_overall, duration_sec_agent,
        customer_msg_num, agent_reply_num, msg_text, closed_without_reply_flag
    FROM agent_message_log
)

SELECT
    fl.*,
    ta.ticket_created_at,
    ta.requester_id,
    ta.user_id,
    ta.ticket_brand,
    ta.ticket_form_type,
    ta.ticket_channel,
    ta.ticket_subject,
    ta.status,
    ta.request_type,
    ta.subtype,
    ta.survey_offered,
    ta.survey_submitted,
    ta.refund_tag,
    ta.refund_eligible,
    ta.refund_not_eligible,
    ta.auto_involved,
    ta.auto_resolved,
    ta.tech_team_involved,
    tech_team.tech_team_duration_sec,
    ta.resolution_time,
    ca.csat_val,
    ad.agent_group,
    ad.agent_name,
    CASE
        WHEN MAX(fl.agent_reply_num) OVER (PARTITION BY fl.ticket_id) = 1 THEN 1
        ELSE 0
    END AS is_fcr
FROM final_log fl
    LEFT JOIN tickets_attr ta ON ta.ticket_id = fl.ticket_id
    LEFT JOIN csat_attr ca ON ca.ticket_id = fl.ticket_id
                         AND ca.csat_rn = 1
                         AND fl.created_at = ca.created_at
    LEFT JOIN agents_dict ad ON ad.agent_id = fl.author_id
    LEFT JOIN tech_team ON fl.ticket_id = tech_team.ticket_id
ORDER BY ticket_id, created_at, COALESCE(msg_created_at, assign_created_at)

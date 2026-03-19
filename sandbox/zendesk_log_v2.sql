WITH
    tickets_to_exclude AS (
SELECT ticket_id as ticket_to_exclude_id, MIN(CAST(created_at AS DATE)) as created_date
FROM data_bronze_zendesk_prod.zendesk_audit
WHERE 1=1
    AND created_at >= DATE '2026-01-01'
    AND events__field_name = 'tags'
    AND (
       events__value LIKE '%cancellation_notification%'
    OR events__value LIKE '%closed_by_merge%'
    OR events__value LIKE '%voice_abandoned_in_voicemail%'
    OR events__value LIKE '%appfollow%'
    OR events__value LIKE '%spam%'
    OR events__value LIKE '%ai_cb_triggered%'
    OR events__value LIKE '%chargeback_precom%'
    OR events__value LIKE '%chargeback_postcom%'
    )
GROUP BY 1
),
    tickets AS (
SELECT
    ticket_id,
    MIN(created_at) AS ticket_created_at,
    CAST(MAX(events__value) AS BIGINT) AS requester_id
FROM data_bronze_zendesk_prod.zendesk_audit
WHERE events__type = 'Create'
  AND events__field_name = 'requester_id'
GROUP BY ticket_id
HAVING MIN(CAST(created_at AS DATE)) >= DATE '2026-01-01'
   AND MIN(CAST(created_at AS DATE)) < current_date
),
    base_audit AS (
SELECT
    za.ticket_id,
    za.channel,
    date_add('hour', 2, za.created_at) as created_at,
    date_trunc('minute', date_add('hour', 2, za.created_at)) as created_at_truncated,
    CAST(CAST(za.author_id AS DOUBLE) AS BIGINT) AS author_id,
    CAST(CAST(za.events__author_id AS DOUBLE) AS BIGINT) AS event_author_id,
    za.events__id,
    za.events__type,
    za.events__field_name,
    za.events__value,
    za.events__previous_value,
    za.events__body,
    za.events__public,
    za.events__from_title,
    CASE WHEN events__type = 'Notification' AND events__from_title IN (
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
                                                                      )
            THEN 1 /* auto notification */
         WHEN CAST(CAST(za.events__author_id AS DOUBLE) AS BIGINT) is not null AND events__public = TRUE
            THEN 2 /* public message */
            ELSE 0
    END is_public_communication
FROM data_bronze_zendesk_prod.zendesk_audit za
    JOIN tickets ON tickets.ticket_id = za.ticket_id
    LEFT JOIN tickets_to_exclude ON tickets_to_exclude.ticket_to_exclude_id = za.ticket_id
WHERE 1=1
  AND za.ticket_id = 816738
  AND tickets_to_exclude.ticket_to_exclude_id IS NULL
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
    ) AS t (
        agent_id,
        agent_name,
        agent_group
            )
),
    tickets_attr AS (
SELECT
-------------------------------------------------
/* base details */
-------------------------------------------------
       ticket_id,
       tickets.ticket_created_at,
       CAST(CAST(tickets.requester_id AS DOUBLE) AS BIGINT) as requester_id,
       MAX(CASE WHEN events__field_name IN (
                                             '32351109113361', /* backoffice */
                                             '40831328206865', /* app_user_id */
                                             '32351085497873' /* supabase */
                                            )
                                        THEN events__value END
       ) as user_id,
       MAX(CASE WHEN events__type = 'Create' AND events__field_name = 'brand_id' THEN
               CASE WHEN events__value = '26467992035601' THEN 'MindScape'
                    WHEN events__value = '27810244289553' THEN 'Neurolift'
                    WHEN events__value = '26468032413713' THEN 'SmartyMe'
                    WHEN events__value = '26222456156689' THEN 'StellarTech Limited'
                    WHEN events__value = '43023476289553' THEN 'Nexera'
                    ELSE 'Unknown'
                    END
           END) as ticket_brand,
       MAX(CASE WHEN events__type = 'Create' AND events__field_name = 'ticket_form_id' THEN
               CASE WHEN events__value = '26472204214801' THEN 'main ticket form'
                    WHEN events__value = '34833592831505' THEN 'in-app ticket form'
                    WHEN events__value = '34902185196177' THEN 'test form'
                    WHEN events__value = '26222488220945' THEN 'default ticket form'
                    WHEN events__value = '35743604923281' THEN 'registration form'
                    ELSE null
                    END
           END) as ticket_form_type,
       MAX(CASE WHEN events__type = 'Create' AND events__field_name = 'requester_id' THEN channel END) as ticket_channel,
       MAX(CASE WHEN events__type = 'Create' AND events__field_name = 'subject' THEN events__value END) as ticket_subject,
       ELEMENT_AT(
           ARRAY_AGG(CASE WHEN events__value = '26222456191633' THEN 'new'
                          WHEN events__value = '26222456196881' THEN 'open'
                          WHEN events__value = '26222488415249' THEN 'in progress'
                          WHEN events__value = '26471687892881' THEN 'waiting for cs'
                          WHEN events__value = '26222456200081' THEN 'pending'
                          WHEN events__value = '26471092160657' THEN 'waiting for customer'
                          WHEN events__value = '26471128667153' THEN 'waiting for tech team'
                          WHEN events__value = '26222456206737' THEN 'solved'
                          WHEN events__value = '26471115820177' THEN 'solved (no reply)'
                          WHEN events__value = 'closed' THEN 'closed'
                         ELSE null
                     END
               ORDER BY created_at DESC, events__id DESC)
           FILTER (WHERE (events__field_name = 'custom_status_id' AND events__value IS NOT NULL) OR
                         (events__field_name = 'status' AND events__value = 'closed')
                   ), 1
       ) as status,
       ELEMENT_AT(
           ARRAY_AGG(events__value ORDER BY created_at DESC, events__id DESC)
           FILTER (WHERE events__field_name = '26442658996241' AND events__value IS NOT NULL), 1
       ) as request_type,
       ELEMENT_AT(
           ARRAY_AGG(events__value ORDER BY created_at DESC, events__id DESC)
           FILTER (WHERE events__field_name = '31320582354705' AND events__value IS NOT NULL), 1
       ) as subtype,

       ELEMENT_AT(
           ARRAY_AGG(events__value ORDER BY created_at DESC, events__id DESC)
           FILTER (WHERE events__field_name = 'assignee_id' AND events__value IS NOT NULL), -1
       ) as assigned_to,
       ELEMENT_AT(
           ARRAY_AGG(author_id ORDER BY created_at DESC, events__id DESC)
           FILTER (WHERE events__type = 'Comment' AND events__public = true), 1
       ) as resolved_by, /* считаем по последней коммуникации с клиентом, кроме паблик комментов есть еще нотификации - учесть здесь */
       COUNT(DISTINCT CASE WHEN events__field_name = 'assignee_id' AND events__value IS NOT NULL THEN events__value END) as assignees_number,
       COUNT(CASE WHEN (
                        events__type = 'Comment' AND
                        events__public = True AND
                        event_author_id <> requester_id
                        )
                       OR
                       (
                        is_public_communication = 1
                       )
                       THEN events__id
       END) as replies_number,
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
       ) as resolution_time,
       MAX(CASE WHEN events__type = 'SurveyOffered' THEN 1  ELSE 0 END) as survey_offered,
       MAX(CASE WHEN events__type = 'SurveyResponseSubmitted' THEN 1  ELSE 0 END) as survey_submitted,
       MAX(CASE WHEN events__field_name = 'tags' AND events__value LIKE '%refund%'     THEN 1 ELSE 0 END) as refund_tag,
       MAX(CASE WHEN events__field_name = 'tags' AND events__value LIKE '%refund_not_eligible%' THEN 1 ELSE 0 END) as refund_not_eligible,
       MAX(CASE WHEN events__field_name = 'tags' AND events__value LIKE '%refund_eligible%'     THEN 1 ELSE 0 END) as refund_eligible
FROM tickets
    JOIN base_audit USING(ticket_id)
WHERE 1=1
GROUP BY 1, 2, 3
),

/* =========================================================
1. Public messages from base_audit
========================================================= */
message_events AS (
    SELECT
        b.ticket_id,
        b.created_at,
        b.events__id AS event_id,
        CAST(CAST(b.author_id AS DOUBLE) AS BIGINT) AS author_id,
        t.requester_id,
        CASE
            WHEN CAST(CAST(b.author_id AS DOUBLE) AS BIGINT) = t.requester_id THEN 'customer_message'
            WHEN ad.agent_id IS NOT NULL THEN 'agent_message'
            ELSE 'unknown'
        END AS event_type,
        b.events__body AS msg_text
    FROM base_audit b
    JOIN tickets t
      ON t.ticket_id = b.ticket_id
    LEFT JOIN agents_dict ad
      ON ad.agent_id = CAST(CAST(b.author_id AS DOUBLE) AS BIGINT)
    WHERE b.is_public_communication IN (1, 2)
),

/* =========================================================
2. Assignment events
========================================================= */
assignment_events AS (
    SELECT
        b.ticket_id,
        b.created_at,
        b.events__id AS event_id,
        CAST(CAST(b.author_id AS DOUBLE) AS BIGINT) AS assignment_author_id,
        t.requester_id,
        CAST(b.events__value AS BIGINT) AS assigned_agent_id,
        'assignment' AS event_type
    FROM base_audit b
    JOIN tickets t
      ON t.ticket_id = b.ticket_id
    WHERE b.events__field_name = 'assignee_id'
      AND b.events__value IS NOT NULL
),

/* =========================================================
3. Unified meaningful event stream
   only:
   - customer_message
   - agent_message
   - assignment
========================================================= */
all_events AS (
    SELECT
        m.ticket_id,
        m.created_at,
        m.event_id,
        m.author_id,
        m.requester_id,
        CAST(NULL AS BIGINT) AS assigned_agent_id,
        CAST(NULL AS BIGINT) AS assignment_author_id,
        m.event_type,
        m.msg_text,
        CASE
            WHEN m.event_type = 'customer_message' THEN 1
            WHEN m.event_type = 'agent_message' THEN 3
        END AS event_priority
    FROM message_events m
    WHERE m.event_type IN ('customer_message', 'agent_message')

    UNION ALL

    SELECT
        a.ticket_id,
        a.created_at,
        a.event_id,
        a.assigned_agent_id AS author_id,
        a.requester_id,
        a.assigned_agent_id,
        a.assignment_author_id,
        a.event_type,
        CAST(NULL AS VARCHAR) AS msg_text,
        2 AS event_priority
    FROM assignment_events a
),

/* =========================================================
4. Ordered stream
========================================================= */
ordered_events AS (
    SELECT
        e.*,
        ROW_NUMBER() OVER (
            PARTITION BY e.ticket_id
            ORDER BY e.created_at, e.event_priority, e.event_id
        ) AS event_seq,

        LAG(e.event_type) OVER (
            PARTITION BY e.ticket_id
            ORDER BY e.created_at, e.event_priority, e.event_id
        ) AS prev_event_type,

        LAG(e.author_id) OVER (
            PARTITION BY e.ticket_id
            ORDER BY e.created_at, e.event_priority, e.event_id
        ) AS prev_author_id,

        LEAD(e.created_at) OVER (
            PARTITION BY e.ticket_id
            ORDER BY e.created_at, e.event_priority, e.event_id
        ) AS next_event_at_any
    FROM all_events e
),

/* =========================================================
5. Customer block numbering
   New block = customer_message after non-customer_message
========================================================= */
customer_marked AS (
    SELECT
        oe.*,
        CASE
            WHEN oe.event_type = 'customer_message'
             AND COALESCE(oe.prev_event_type, 'none') <> 'customer_message'
            THEN 1
            ELSE 0
        END AS is_new_customer_block
    FROM ordered_events oe
),

customer_numbered AS (
    SELECT
        cm.*,
        SUM(cm.is_new_customer_block) OVER (
            PARTITION BY cm.ticket_id
            ORDER BY cm.created_at, cm.event_priority, cm.event_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS customer_msg_num
    FROM customer_marked cm
),

customer_enriched AS (
    SELECT
        cn.*,
        MAX(
            CASE WHEN cn.is_new_customer_block = 1 THEN cn.created_at END
        ) OVER (
            PARTITION BY cn.ticket_id
            ORDER BY cn.created_at, cn.event_priority, cn.event_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS current_customer_block_start_at
    FROM customer_numbered cn
),

/* =========================================================
6. Mark first agent reply after open customer block
========================================================= */
reply_candidates AS (
    SELECT
        ce.*,
        CASE
            WHEN ce.event_type = 'agent_message'
             AND ce.customer_msg_num > 0
             AND ce.current_customer_block_start_at IS NOT NULL
             AND COALESCE(ce.prev_event_type, 'none') <> 'agent_message'
            THEN 1
            ELSE 0
        END AS is_first_agent_reply
    FROM customer_enriched ce
),

reply_numbered AS (
    SELECT
        rc.*,
        CASE
            WHEN rc.is_first_agent_reply = 1 THEN
                SUM(rc.is_first_agent_reply) OVER (
                    PARTITION BY rc.ticket_id
                    ORDER BY rc.created_at, rc.event_priority, rc.event_id
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )
        END AS agent_reply_num
    FROM reply_candidates rc
),

/* =========================================================
7. First reply timestamp per customer block
========================================================= */
first_reply_per_block AS (
    SELECT
        ticket_id,
        customer_msg_num,
        MIN(created_at) AS first_reply_at
    FROM reply_numbered
    WHERE is_first_agent_reply = 1
    GROUP BY 1, 2
),

/* =========================================================
8. Customer messages with row number inside block
========================================================= */
customer_messages_ranked AS (
    SELECT
        rn.ticket_id,
        rn.event_id,
        rn.author_id,
        rn.created_at,
        rn.msg_text,
        rn.customer_msg_num,
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

/* =========================================================
9. Previous first-agent-reply before first customer msg in block
========================================================= */
customer_block_prev_reply AS (
    SELECT
        cmr.ticket_id,
        cmr.customer_msg_num,
        MAX(r.created_at) AS previous_agent_reply_at
    FROM customer_messages_ranked cmr
    LEFT JOIN reply_numbered r
      ON r.ticket_id = cmr.ticket_id
     AND r.is_first_agent_reply = 1
     AND r.created_at < cmr.created_at
    WHERE cmr.rn_in_block = 1
    GROUP BY 1, 2
),

/* =========================================================
10. Customer message log
   one row per physical customer message
========================================================= */
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
        fr.first_reply_at AS next_event_at,
        CASE
            WHEN cmr.rn_in_block = 1
                THEN CASE
                    WHEN COALESCE(cbpr.previous_agent_reply_at, cmr.created_at) = cmr.created_at THEN 0
                    ELSE DATE_DIFF('second', COALESCE(cbpr.previous_agent_reply_at, cmr.created_at), cmr.created_at)
                END
            ELSE DATE_DIFF('second', cmr.prev_customer_msg_at, cmr.created_at)
        END AS duration_sec_overall,
        CAST(NULL AS BIGINT) AS duration_sec_agent,
        cmr.customer_msg_num,
        CAST(NULL AS BIGINT) AS agent_reply_num,
        cmr.msg_text,
        CASE WHEN cmr.rn_in_block = 1 THEN 1 ELSE 0 END AS is_first_message_in_block,
        MIN(cmr.created_at) OVER (
            PARTITION BY cmr.ticket_id, cmr.customer_msg_num
        ) AS customer_block_start_at
    FROM customer_messages_ranked cmr
    LEFT JOIN customer_block_prev_reply cbpr
      ON cbpr.ticket_id = cmr.ticket_id
     AND cbpr.customer_msg_num = cmr.customer_msg_num
    LEFT JOIN first_reply_per_block fr
      ON fr.ticket_id = cmr.ticket_id
     AND fr.customer_msg_num = cmr.customer_msg_num
),

/* =========================================================
11. First replies
========================================================= */
first_replies AS (
    SELECT
        rn.ticket_id,
        rn.event_id,
        rn.author_id,
        rn.created_at AS reply_created_at,
        rn.msg_text,
        rn.customer_msg_num,
        rn.agent_reply_num,
        rn.current_customer_block_start_at AS customer_block_start_at,
        rn.next_event_at_any AS next_event_at
    FROM reply_numbered rn
    WHERE rn.is_first_agent_reply = 1
),

/* =========================================================
12. Latest assignment on same agent <= reply
   same timestamp is allowed
========================================================= */
reply_assignment_candidates AS (
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
        ae.created_at AS assign_created_at,
        ROW_NUMBER() OVER (
            PARTITION BY fr.ticket_id, fr.event_id
            ORDER BY ae.created_at DESC, ae.event_id DESC
        ) AS rn_desc
    FROM first_replies fr
    LEFT JOIN assignment_events ae
      ON ae.ticket_id = fr.ticket_id
     AND ae.assigned_agent_id = fr.author_id
     AND ae.created_at <= fr.reply_created_at
),

last_assignment_before_reply AS (
    SELECT
        ticket_id,
        event_id,
        author_id,
        reply_created_at,
        msg_text,
        customer_msg_num,
        agent_reply_num,
        customer_block_start_at,
        next_event_at,
        assign_created_at
    FROM reply_assignment_candidates
    WHERE rn_desc = 1
),

/* =========================================================
13. Validate assignment for reply
   valid if no reassignment to another agent after assignment and before/equal reply
========================================================= */
reply_with_assignment_status AS (
    SELECT
        lar.ticket_id,
        lar.event_id,
        lar.author_id,
        lar.reply_created_at,
        lar.msg_text,
        lar.customer_msg_num,
        lar.agent_reply_num,
        lar.customer_block_start_at,
        lar.next_event_at,
        lar.assign_created_at,
        CASE
            WHEN lar.assign_created_at IS NOT NULL
             AND NOT EXISTS (
                SELECT 1
                FROM assignment_events ae2
                WHERE ae2.ticket_id = lar.ticket_id
                  AND ae2.created_at > lar.assign_created_at
                  AND ae2.created_at <= lar.reply_created_at
                  AND ae2.assigned_agent_id <> lar.author_id
             )
            THEN 1
            ELSE 0
        END AS has_valid_assignment
    FROM last_assignment_before_reply lar
),

/* =========================================================
14. Last any assignment before reply
   for previous_event_at in reply_without_assignment
========================================================= */
last_any_assignment_before_reply AS (
    SELECT
        fr.ticket_id,
        fr.event_id,
        MAX(ae.created_at) AS last_any_assign_at
    FROM first_replies fr
    LEFT JOIN assignment_events ae
      ON ae.ticket_id = fr.ticket_id
     AND ae.created_at <= fr.reply_created_at
    GROUP BY 1, 2
),

/* =========================================================
15. reply_by_assignment_after_customer_msg
========================================================= */
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
        CAST(NULL AS INTEGER) AS is_first_message_in_block,
        rwas.customer_block_start_at
    FROM reply_with_assignment_status rwas
    WHERE rwas.has_valid_assignment = 1
),

/* =========================================================
16. reply_without_assignment
   overall always from first customer message in block
   agent duration = 0
========================================================= */
reply_without_assignment_log AS (
    SELECT
        rwas.reply_created_at AS created_at,
        rwas.ticket_id,
        'reply_without_assignment' AS log_type,
        rwas.author_id,
        rwas.reply_created_at AS msg_created_at,
        CAST(NULL AS TIMESTAMP) AS assign_created_at,
        COALESCE(laabr.last_any_assign_at, rwas.customer_block_start_at) AS previous_event_at,
        rwas.next_event_at,
        DATE_DIFF('second', rwas.customer_block_start_at, rwas.reply_created_at) AS duration_sec_overall,
        0 AS duration_sec_agent,
        rwas.customer_msg_num,
        rwas.agent_reply_num,
        rwas.msg_text,
        CAST(NULL AS INTEGER) AS is_first_message_in_block,
        rwas.customer_block_start_at
    FROM reply_with_assignment_status rwas
    LEFT JOIN last_any_assignment_before_reply laabr
      ON laabr.ticket_id = rwas.ticket_id
     AND laabr.event_id = rwas.event_id
    WHERE rwas.has_valid_assignment = 0
),

/* =========================================================
17. Non-first agent messages must stay in final log
========================================================= */
agent_message_log AS (
    SELECT
        rn.created_at AS created_at,
        rn.ticket_id,
        'agent_message' AS log_type,
        rn.author_id,
        rn.created_at AS msg_created_at,
        CAST(NULL AS TIMESTAMP) AS assign_created_at,
        CAST(NULL AS TIMESTAMP) AS previous_event_at,
        rn.next_event_at_any AS next_event_at,
        CAST(NULL AS BIGINT) AS duration_sec_overall,
        CAST(NULL AS BIGINT) AS duration_sec_agent,
        rn.customer_msg_num,
        CAST(NULL AS BIGINT) AS agent_reply_num,
        rn.msg_text,
        CAST(NULL AS INTEGER) AS is_first_message_in_block,
        rn.current_customer_block_start_at AS customer_block_start_at
    FROM reply_numbered rn
    WHERE rn.event_type = 'agent_message'
      AND COALESCE(rn.is_first_agent_reply, 0) = 0
),

/* =========================================================
18. Assignment boundaries
   break on:
   - any next public message
   - next assignment
   same-timestamp reply by same agent closes assignment
========================================================= */
assignment_boundaries AS (
    SELECT
        ae.ticket_id,
        ae.event_id,
        ae.assigned_agent_id AS author_id,
        ae.created_at AS assign_created_at,

        MIN(CASE
            WHEN (
                    ev.created_at > ae.created_at
                    OR (ev.created_at = ae.created_at AND ev.event_priority > 2)
                 )
             AND ev.event_type IN ('customer_message', 'agent_message')
            THEN ev.created_at
        END) AS next_public_message_at,

        MIN(CASE
            WHEN (
                    ev.created_at > ae.created_at
                    OR (ev.created_at = ae.created_at AND ev.event_priority > 2)
                 )
             AND ev.event_type = 'assignment'
            THEN ev.created_at
        END) AS next_assignment_at,

        MIN(CASE
            WHEN (
                    ev.created_at > ae.created_at
                    OR (ev.created_at = ae.created_at AND ev.event_priority > 2)
                 )
             AND ev.event_type = 'agent_message'
             AND ev.author_id = ae.assigned_agent_id
            THEN ev.created_at
        END) AS next_same_agent_reply_at

    FROM assignment_events ae
    LEFT JOIN all_events ev
      ON ev.ticket_id = ae.ticket_id
    GROUP BY 1,2,3,4
),

assignment_status AS (
    SELECT
        ab.*,
        LEAST(
            COALESCE(ab.next_public_message_at, TIMESTAMP '9999-12-31 00:00:00'),
            COALESCE(ab.next_assignment_at, TIMESTAMP '9999-12-31 00:00:00')
        ) AS break_at,
        LEAST(
            COALESCE(ab.next_public_message_at, TIMESTAMP '9999-12-31 00:00:00'),
            COALESCE(ab.next_assignment_at, TIMESTAMP '9999-12-31 00:00:00')
        ) AS next_event_at
    FROM assignment_boundaries ab
),

assignment_without_reply_raw AS (
    SELECT
        ast.ticket_id,
        ast.event_id,
        ast.author_id,
        ast.assign_created_at,
        ast.next_event_at
    FROM assignment_status ast
    WHERE ast.next_same_agent_reply_at IS NULL
       OR ast.next_same_agent_reply_at > ast.break_at
),

/* =========================================================
19. Previous anchor for empty assignments
   previous_event_at = last unanswered assignment OR current customer block start
========================================================= */
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

current_customer_block_for_assignment AS (
    SELECT
        awr.ticket_id,
        awr.event_id,
        MAX(cml.customer_block_start_at) AS customer_block_start_at
    FROM assignment_without_reply_raw awr
    LEFT JOIN customer_message_log cml
      ON cml.ticket_id = awr.ticket_id
     AND cml.customer_block_start_at <= awr.assign_created_at
    GROUP BY 1,2
),

current_customer_num_for_assignment AS (
    SELECT
        awr.ticket_id,
        awr.event_id,
        MAX(cml.customer_msg_num) AS customer_msg_num
    FROM assignment_without_reply_raw awr
    LEFT JOIN customer_message_log cml
      ON cml.ticket_id = awr.ticket_id
     AND cml.msg_created_at <= awr.assign_created_at
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
        COALESCE(laba.last_prev_assign_at, ccba.customer_block_start_at) AS previous_event_at,
        awr.next_event_at,
        DATE_DIFF('second', awr.assign_created_at, awr.next_event_at) AS duration_sec_overall,
        CAST(NULL AS BIGINT) AS duration_sec_agent,
        ccna.customer_msg_num,
        CAST(NULL AS BIGINT) AS agent_reply_num,
        CAST(NULL AS VARCHAR) AS msg_text,
        CAST(NULL AS INTEGER) AS is_first_message_in_block,
        ccba.customer_block_start_at
    FROM assignment_without_reply_raw awr
    LEFT JOIN last_any_assignment_before_assignment laba
      ON laba.ticket_id = awr.ticket_id
     AND laba.event_id = awr.event_id
    LEFT JOIN current_customer_block_for_assignment ccba
      ON ccba.ticket_id = awr.ticket_id
     AND ccba.event_id = awr.event_id
    LEFT JOIN current_customer_num_for_assignment ccna
      ON ccna.ticket_id = awr.ticket_id
     AND ccna.event_id = awr.event_id
)

/* =========================================================
20. Final unified log
========================================================= */
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
    msg_text
FROM customer_message_log

UNION ALL

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
    msg_text
FROM reply_by_assignment_log

UNION ALL

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
    msg_text
FROM reply_without_assignment_log

UNION ALL

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
    msg_text
FROM assignment_without_reply_log

UNION ALL

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
    msg_text
FROM agent_message_log
;
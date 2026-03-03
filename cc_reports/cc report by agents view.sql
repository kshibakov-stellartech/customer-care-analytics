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
    tickets.requester_id as base_requester_id,
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
  --AND za.ticket_id = 819071
  AND tickets_to_exclude.ticket_to_exclude_id IS NULL
),
    agents_dict AS (
    SELECT *
    FROM (
        VALUES
            (41972533108625, 'Konstantin Shibakov', 'Admins'),
            (40215157462161, 'QA', 'Admins'),
            (39272670052113, 'Sam Bondar', 'Moon Rangers'),
            (38754864964753, 'Brian Tepliuk', 'Moon Rangers'),
            (38694917174545, 'Mike Mkrtumyan', 'Moon Rangers'),
            (38657563018769, 'Alice Sakharova', 'Moon Rangers'),
            (38022764826129, 'Allie Kostukovich', 'Blanc'),
            (38022759246737, 'Kate Rumiantseva', 'Moon Rangers'),
            (37992873903889, 'Ann Dereka', 'Moon Rangers'),
            (36064560830737, 'Mykyta', 'Admins'),
            (35310711957393, 'Anette Monaselidze', 'Blanc'),
            (35219779434897, 'Ilia Tregubov', 'Admins'),
            (34224285677201, 'Yaroslav Kukharenko', 'Admins'),
            (33602186941713, 'Jackie Si', 'Blanc'),
            (33118701264017, 'Daria Saranchova', 'Blanc'),
            (33118711659921, 'Katrina Novikova', 'Blanc'),
            (31467436910865, 'Jenny', 'Moon Rangers'),
            (30786139608081, 'Jade Kasper', 'Blanc'),
            (30655366698001, 'Catherine Moroz', 'Blanc'),
            (30648746936465, 'Alexander Petrov', 'Moon Rangers'),
            (30160506886161, 'Alex Poponin', 'Blanc'),
            (29737848444689, 'Daniel Vinokurov', 'Blanc'),
            (26440502459665, 'Nikki', 'Automation'),
            (26349132549521, 'Mia Petchenko', 'Moon Rangers'),
            (26222438547857, 'Maksym Zvieriev', 'Blanc'),

            (42676049623057, 'Sophie Palamarchuk', 'Moon Rangers'),
            (42676111579153, 'Michael Brodovskyi', 'Moon Rangers')
    ) AS t (
        agent_id,
        agent_name,
        agent_group
            )
),
    csat_attr AS (
  --tech_team_time, подзапрос для расчетов
SELECT ticket_id,
       created_at,
       events__id,
       author_id as csat_author_id,
       csat_val,
       ROW_NUMBER() OVER(PARTITION BY ticket_id ORDER BY created_at DESC) as csat_rn
FROM (
  SELECT ticket_id,
         created_at,
         author_id,
         events__id,
         events__type,
         events__field_name,
         events__value,
         LEAD(events__field_name) OVER(PARTITION BY ticket_id ORDER BY created_at) as csat_flag,
         LEAD(events__value) OVER(PARTITION BY ticket_id ORDER BY created_at) as csat_val
  FROM base_audit
  WHERE 1=1
    AND (
         (events__type = 'Comment' AND author_id <> base_requester_id) OR
         (events__type = 'Change'  AND events__field_name = 'satisfaction_score' AND events__value IN ('good', 'bad'))
        )
ORDER BY ticket_id, created_at
) raw_csat
WHERE 1=1
  AND events__type = 'Comment'
  AND csat_flag = 'satisfaction_score'

),
    all_msg AS (
SELECT CASE WHEN b.event_author_id = t.requester_id THEN 'requester' ELSE 'agent' END as log_type,
       b.ticket_id,
       t.requester_id,
       b.created_at as assign_created_at,
       b.created_at + INTERVAL '5' SECOND as msg_created_at,
       COALESCE(LAG(b.created_at + INTERVAL '5' SECOND) OVER (PARTITION BY b.ticket_id ORDER BY b.created_at), b.created_at) AS prev_msg_created_at,
       COALESCE(DATE_DIFF('second', LAG(b.created_at) OVER (PARTITION BY b.ticket_id ORDER BY b.created_at), b.created_at), 0) AS response_duration_sec,
       CAST(CAST(b.author_id AS DOUBLE) AS BIGINT) AS msg_author_id,
       b.events__id msg_event_id,
       b.events__body as msg_text
FROM tickets t
    JOIN base_audit b ON t.ticket_id = b.ticket_id
                     AND b.is_public_communication IN (1, 2)
),
    agent_assign AS (
SELECT
    b.ticket_id,
    t.requester_id,
    CAST(b.events__value AS BIGINT) AS author_id_value,
    b.created_at,
    b.created_at_truncated,
    COALESCE(LEAST(
                    LEAD(b.created_at_truncated) OVER(PARTITION BY b.ticket_id, b.events__value ORDER BY b.created_at),
                    LEAD(b.created_at_truncated) OVER (PARTITION BY b.ticket_id ORDER BY b.created_at)
             ), b.created_at_truncated + INTERVAL '14' DAY
    ) AS next_assign_by_agent,

    LEAD(b.created_at_truncated) OVER(PARTITION BY b.ticket_id, b.events__value ORDER BY b.created_at) as next_1,
    LEAD(b.created_at_truncated) OVER (PARTITION BY b.ticket_id ORDER BY b.created_at) as next_2,
    b.created_at_truncated + INTERVAL '14' DAY as next_3,


    COALESCE(LEAD(b.created_at) OVER (PARTITION BY b.ticket_id ORDER BY b.created_at), b.created_at) AS next_assign,
    COALESCE(DATE_DIFF('second', b.created_at, LEAD(b.created_at) OVER (PARTITION BY b.ticket_id ORDER BY b.created_at)), 0) AS assign_duration_sec,
    CAST(CAST(b.author_id AS DOUBLE) AS BIGINT) AS author_id,
    b.events__id
FROM base_audit b
    JOIN tickets t ON t.ticket_id = b.ticket_id
WHERE 1=1
  AND b.events__field_name = 'assignee_id' AND b.events__value IS NOT NULL
),

    full_log AS (
SELECT ticket_id,
       created_at,
       log_type,
       assign_event_id as event_id,
       msg_author_id as author_id,
       COALESCE(assign_duration_sec, 0) as duration_sec,
       msg_text,
       agent_msg_rn,
       MAX(customer_msg_rn) OVER (PARTITION BY ticket_id ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
       AS msg_rn
FROM
(
SELECT log_type,
       ticket_id,
       assign_created_at as created_at,
       assign_created_at,
       msg_event_id as assign_event_id,
       requester_id as assign_author_id,
       null as assign_duration_sec,
       response_duration_sec,
       null as next_assign,
       msg_created_at,
       prev_msg_created_at,
       msg_author_id,
       msg_event_id,
       msg_text,
       ROW_NUMBER() OVER (PARTITION BY ticket_id ORDER BY msg_created_at) as customer_msg_rn,
       null as agent_msg_rn
FROM all_msg
WHERE 1=1
  AND log_type = 'requester'
UNION ALL
SELECT CASE WHEN b.msg_author_id is null THEN 'agent_to_check' ELSE 'agent' END as log_type,
       agent_assign.ticket_id,
       COALESCE(b.assign_created_at, agent_assign.created_at) as created_at,
       agent_assign.created_at as assign_created_at,
       agent_assign.events__id as assign_event_id,
       author_id_value as assign_author_id,
       CASE WHEN b.msg_author_id is null then agent_assign.assign_duration_sec
            ELSE DATE_DIFF('second', agent_assign.created_at, COALESCE(b.msg_created_at, agent_assign.created_at))
       END as assign_duration_sec,
       response_duration_sec,
       next_assign,
       COALESCE(b.msg_created_at, agent_assign.created_at) as msg_created_at,
       prev_msg_created_at,
       COALESCE(CAST(CAST(b.msg_author_id AS DOUBLE) AS BIGINT), agent_assign.author_id_value) AS msg_author_id,
       b.msg_event_id,
       b.msg_text,
       null as customer_msg_rn,
       CASE WHEN b.msg_author_id is not null THEN ROW_NUMBER() OVER (PARTITION BY t.ticket_id ORDER BY msg_created_at) END as agent_msg_rn
FROM agent_assign
  JOIN tickets t ON t.ticket_id = agent_assign.ticket_id
  LEFT JOIN all_msg b ON b.ticket_id = agent_assign.ticket_id
                        AND b.msg_author_id = agent_assign.author_id_value
                        AND t.requester_id <> b.msg_author_id
                        AND b.msg_created_at >= agent_assign.created_at_truncated
                        AND b.msg_created_at < agent_assign.next_assign_by_agent
WHERE 1=1
UNION ALL
SELECT 'agent_wo_assign' as log_type,
       b.ticket_id,
       b.assign_created_at as created_at,
       b.assign_created_at,
       null as assign_event_id,
       null as assign_author_id,
       0 as assign_duration_sec,
       response_duration_sec,
       null as next_assign,
       b.msg_created_at as msg_created_at,
       b.prev_msg_created_at,
       CAST(CAST(b.msg_author_id AS DOUBLE) AS BIGINT) AS msg_author_id,
       b.msg_event_id,
       b.msg_text,
       null as customer_msg_rn,
       null as agent_msg_rn
FROM all_msg b
    LEFT JOIN agent_assign ON b.ticket_id = agent_assign.ticket_id
                          AND b.msg_author_id = agent_assign.author_id_value
                          AND b.msg_created_at >= agent_assign.created_at_truncated
                          AND b.msg_created_at < agent_assign.next_assign_by_agent
WHERE 1=1
  AND agent_assign.ticket_id is null
  AND b.log_type <> 'requester'
) raw_log
),
    ticket_log_attr AS (
SELECT ticket_id,
       MAX(msg_rn) as msg_from_customer_count
FROM full_log ta
GROUP BY 1
),

     res AS (
SELECT ad.agent_name,
       ad.agent_group,
       ad.agent_id,
       CASE WHEN log_type = 'agent' AND msg_rn = 1 AND agent_msg_rn = 1 THEN 1 ELSE 0 END as is_frt,
       CASE WHEN msg_from_customer_count <= 1 THEN 1 ELSE 0 END as is_fcr,
       CASE WHEN log_type = 'agent' AND msg_rn = 2 THEN 1 ELSE 0 END as is_srt,
       fl.created_at,
       DATE_TRUNC('week', fl.created_at) as week_dt,
       fl.ticket_id,
       fl.log_type,
       fl.msg_text,
       fl.msg_rn as customer_msg_rn,
       fl.agent_msg_rn,
       fl.duration_sec,
       ca.csat_val
FROM agents_dict ad
    JOIN full_log fl ON ad.agent_id = fl.author_id
    LEFT JOIN csat_attr ca ON fl.ticket_id = ca.ticket_id
                          AND ca.csat_rn = 1
                          AND fl.created_at = ca.created_at
    LEFT JOIN ticket_log_attr tla ON fl.ticket_id = tla.ticket_id
                                 AND fl.log_type = 'agent'
                                 AND fl.msg_rn = 1
                                 AND fl.agent_msg_rn = 1
WHERE fl.log_type <> 'requester'
)

SELECT *
FROM res
WHERE 1=1
  AND CAST(created_at AS DATE) BETWEEN DATE '2026-01-23' AND DATE '2026-02-23'
  AND csat_val is not null
  AND agent_id = 42676111579153

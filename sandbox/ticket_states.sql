
--ticket_assignee_type можно считать в репорте
WITH
    base_audit AS (
SELECT
    ticket_id,
    channel,
    date_add('hour', 2, created_at) as created_at,
    date_trunc('minute', date_add('hour', 2, created_at)) as created_at_truncated,
    CAST(CAST(author_id AS DOUBLE) AS BIGINT) AS author_id,
    CAST(CAST(events__author_id AS DOUBLE) AS BIGINT) AS event_author_id,
    events__id,
    events__type,
    events__field_name,
    events__value,
    events__previous_value,
    events__body,
    events__public
FROM data_bronze_zendesk_prod.zendesk_audit
WHERE 1=1
  AND ticket_id = 633212
  --AND created_at >= DATE '2026-01-01'
),
    tickets AS (
SELECT
    ticket_id,
    MIN(created_at) AS ticket_created_at,
    CAST(MAX(events__value) AS BIGINT) AS requester_id
/*
тут считаем сразу все остальные аттрибуты
*/
FROM base_audit
WHERE events__type = 'Create'
  AND events__field_name = 'requester_id'
GROUP BY ticket_id
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
            (26222438547857, 'Maksym Zvieriev', 'Blanc')
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
       COUNT(CASE WHEN events__type = 'Comment' THEN events__id END) as replies_number,
       MAX(CASE WHEN events__type = 'Comment' AND author_id = 26440502459665 THEN 1  ELSE 0 END) as auto_involved,
       CASE WHEN ELEMENT_AT(
           ARRAY_AGG(author_id ORDER BY created_at DESC, events__id DESC)
           FILTER (WHERE events__type = 'Comment' AND events__public = true), 1
       ) = 26440502459665 THEN 1 ELSE 0 END as auto_resolved,
       MAX(CASE WHEN events__field_name = 'tags' AND events__value LIKE '%tech_team%' THEN 1 ELSE 0 END) as tech_team_involved,
       MAX(DATE_DIFF('second', ticket_created_at, CASE WHEN events__field_name = 'custom_status_id' AND events__value = '26222456206737' THEN created_at
                                                       WHEN events__type = 'Comment' AND author_id <> requester_id THEN created_at
                                                  END
                    )
       ) as resolution_time,
       MAX(CASE WHEN events__type = 'SurveyOffered' THEN 1  ELSE 0 END) as survey_offered,
       MAX(CASE WHEN events__type = 'SurveyResponseSubmitted' THEN 1  ELSE 0 END) as survey_submitted
FROM tickets
    JOIN base_audit USING(ticket_id)
WHERE 1=1
GROUP BY 1, 2, 3
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
                     AND b.event_author_id is not null
                     AND b.events__public = TRUE
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
       COALESCE(response_duration_sec, assign_duration_sec) as duration_sec,
       msg_text,
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
       ROW_NUMBER() OVER (PARTITION BY ticket_id ORDER BY msg_created_at) as customer_msg_rn
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
       agent_assign.assign_duration_sec,
       response_duration_sec,
       next_assign,
       COALESCE(b.msg_created_at, agent_assign.created_at) as msg_created_at,
       prev_msg_created_at,
       COALESCE(CAST(CAST(b.msg_author_id AS DOUBLE) AS BIGINT), agent_assign.author_id_value) AS msg_author_id,
       b.msg_event_id,
       b.msg_text,
       null as customer_msg_rn
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
       null assign_duration_sec,
       response_duration_sec,
       null as next_assign,
       b.msg_created_at as msg_created_at,
       b.prev_msg_created_at,
       CAST(CAST(b.msg_author_id AS DOUBLE) AS BIGINT) AS msg_author_id,
       b.msg_event_id,
       b.msg_text,
       null as customer_msg_rn
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
    ticket_log_attr AS (
SELECT ticket_id,
       DATE_DIFF('second', MIN(created_at), MAX(created_at)) as resolution_time,
       DATE_DIFF('second', MIN(created_at), MAX(created_at)) - SUM(CASE WHEN log_type = 'requester' THEN duration_sec END) as handling_time,
       SUM(CASE WHEN log_type = 'agent_to_check' THEN duration_sec END) as lost_time,
       AVG(CASE WHEN log_type = 'agent' THEN duration_sec END) as avg_reply_time,
       ELEMENT_AT(
           ARRAY_AGG(duration_sec ORDER BY created_at)
           FILTER (WHERE log_type = 'agent' AND msg_rn = 1), 1
       ) as first_reply_time,
       ELEMENT_AT(
           ARRAY_AGG(author_id ORDER BY created_at)
           FILTER (WHERE log_type = 'agent' AND msg_rn = 1), 1
       ) as frt_agent,
       ELEMENT_AT(
           ARRAY_AGG(duration_sec ORDER BY created_at)
           FILTER (WHERE log_type = 'agent' AND msg_rn = 2), 1
       ) as second_reply_time,
       ELEMENT_AT(
           ARRAY_AGG(author_id ORDER BY created_at)
           FILTER (WHERE log_type = 'agent' AND msg_rn = 2), 1
       ) as srt_agent,
       AVG(CASE WHEN log_type = 'agent' AND msg_rn > 1 THEN duration_sec END) as concecutive_reply_time,
       CASE WHEN DATE_DIFF('second', MIN(created_at), MAX(created_at)) > 86400 THEN 1 ELSE 0 END as sla_total_resolution, /* 86400 sec = 24 hours */
       CASE WHEN ELEMENT_AT(
           ARRAY_AGG(duration_sec ORDER BY created_at)
           FILTER (WHERE log_type = 'agent' AND msg_rn = 1), 1) > 600 THEN 1 ELSE 0
       END as sla_first_reply, /* 600 sec = 10 min */
       CASE WHEN ELEMENT_AT(
           ARRAY_AGG(duration_sec ORDER BY created_at)
           FILTER (WHERE log_type = 'agent' AND msg_rn = 2), 1) > 1200 THEN 1 ELSE 0
       END as sla_second_reply, /* 1200 sec = 20 min */

       AVG(CASE WHEN log_type = 'agent' AND author_id = 26440502459665 THEN duration_sec END) as avg_reply_time_auto,
       ELEMENT_AT(
           ARRAY_AGG(duration_sec ORDER BY created_at)
           FILTER (WHERE log_type = 'agent' AND msg_rn = 1 AND author_id = 26440502459665), 1
       ) as first_reply_time_auto,
       ELEMENT_AT(
           ARRAY_AGG(duration_sec ORDER BY created_at)
           FILTER (WHERE log_type = 'agent' AND msg_rn = 2 AND author_id = 26440502459665), 1
       ) as second_reply_time_auto,
       AVG(CASE WHEN log_type = 'agent' AND msg_rn > 1  AND author_id = 26440502459665 THEN duration_sec END) as concecutive_reply_time_auto,

       AVG(CASE WHEN log_type = 'agent' AND author_id <> 26440502459665 THEN duration_sec END) as avg_reply_time_person,
       ELEMENT_AT(
           ARRAY_AGG(duration_sec ORDER BY created_at)
           FILTER (WHERE log_type = 'agent' AND msg_rn = 1 AND author_id <> 26440502459665), 1
       ) as first_reply_time_person,
       ELEMENT_AT(
           ARRAY_AGG(duration_sec ORDER BY created_at)
           FILTER (WHERE log_type = 'agent' AND msg_rn = 2 AND author_id <> 26440502459665), 1
       ) as second_reply_time_person,
       AVG(CASE WHEN log_type = 'agent' AND msg_rn > 1  AND author_id <> 26440502459665 THEN duration_sec END) as concecutive_reply_time_person
FROM full_log ta
GROUP BY 1
)

SELECT ta.*,
       csat.rating as survey_rating,
       tla.*
FROM tickets_attr ta
    JOIN ticket_log_attr tla ON ta.ticket_id = tla.ticket_id
    LEFT JOIN data_bronze_zendesk_prod.zendesk_csat csat ON ta.ticket_id = csat.ticket_id

       --survey_rating left join csat,
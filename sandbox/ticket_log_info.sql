WITH
    base_audit AS (
SELECT
    ticket_id,
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
       MAX(customer_msg_rn) OVER (PARTITION BY ticket_id ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
       AS replied_to_customer_msg_rn
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
SELECT 'agent' as log_type,
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
)


--SELECT * FROM base_audit
--SELECT * FROM tickets;
--SELECT * FROM all_msg;
--SELECT * FROM agent_assign;
--SELECT * FROM full_log ORDER BY ticket_id, created_at;

SELECT *
FROM full_log
ORDER BY ticket_id, created_at
;

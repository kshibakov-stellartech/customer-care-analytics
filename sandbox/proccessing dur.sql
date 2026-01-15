WITH base_audit AS (
    SELECT
        ticket_id,
        date_add('hour', 2, created_at) as created_at,
        CAST(author_id AS BIGINT) AS author_id,
        events__id,
        events__type,
        events__field_name,
        events__value,
        events__body,
        events__public
    FROM data_bronze_zendesk_prod.zendesk_audit
    WHERE 1=1
      --AND ticket_id = 633212
      AND created_at >= DATE '2026-01-01'
),

tickets AS (
    SELECT
        ticket_id,
        MIN(created_at) AS ticket_created_at,
        CAST(MAX(events__value) AS BIGINT) AS requester_id
    FROM base_audit
    WHERE events__type = 'Create'
      AND events__field_name = 'requester_id'
    GROUP BY ticket_id
),

/* ---------------------------------------------------
   ASSIGN = границы интервалов + сообщения клиента
--------------------------------------------------- */
assign AS (
    SELECT
        b.ticket_id,
        t.requester_id,
        b.author_id,
        b.created_at,
        COALESCE(
            LEAD(b.created_at) OVER (
                PARTITION BY b.ticket_id
                ORDER BY b.created_at
            ),
            date_add('day', 14, b.created_at)
        ) AS next_assign,
        b.events__id,
        CAST(b.events__value AS BIGINT) AS author_id_value,
        b.events__body,
        CASE
            WHEN b.events__type = 'Comment'
             AND b.author_id = t.requester_id
            THEN 1 ELSE 0
        END AS is_customer_msg
    FROM base_audit b
    JOIN tickets t
      ON b.ticket_id = t.ticket_id
    WHERE
        (b.events__field_name = 'assignee_id' AND b.events__value IS NOT NULL)
        OR
        (b.events__type = 'Comment' AND b.author_id = t.requester_id)
),

/* ---------------------------------------------------
   СООБЩЕНИЯ АГЕНТОВ (ТОЛЬКО Comment)
--------------------------------------------------- */
agent_messages AS (
    SELECT
        b.ticket_id,
        b.created_at,
        b.author_id,
        b.events__body,
        ROW_NUMBER() OVER (
            PARTITION BY b.ticket_id
            ORDER BY b.created_at
        ) AS ticket_reply_num,
        ROW_NUMBER() OVER (
            PARTITION BY b.ticket_id, b.author_id
            ORDER BY b.created_at
        ) AS agent_reply_num
    FROM base_audit b
    JOIN tickets t
      ON b.ticket_id = t.ticket_id
    WHERE b.events__type = 'Comment'
      AND b.author_id <> t.requester_id
      AND events__public = TRUE
),

log_raw AS (

    /* ---------- 1. СООБЩЕНИЯ КЛИЕНТА ---------- */
    SELECT
        a.ticket_id,
        a.requester_id,
        COALESCE(a.author_id_value, a.author_id) AS assignee_id,
        a.events__id AS assignee_event_id,
        a.created_at AS assign_created_at,
        a.next_assign,

        'customer' AS actor_type,
        'msg' AS action_type,

        a.created_at AS action_created_at,
        LAG(a.created_at) OVER (
            PARTITION BY a.ticket_id
            ORDER BY a.created_at
        ) AS last_prev_action_created_at,

        a.author_id AS action_author_id,
        a.events__body AS msg_body,

        NULL AS ticket_reply_num,
        NULL AS agent_reply_num

    FROM assign a
    WHERE a.is_customer_msg = 1


    UNION ALL

    /* ---------- 2. СООБЩЕНИЯ АГЕНТОВ (PUBLIC) ---------- */
    SELECT
        a.ticket_id,
        a.requester_id,
        COALESCE(a.author_id_value, a.author_id) AS assignee_id,
        a.events__id AS assignee_event_id,
        a.created_at AS assign_created_at,
        a.next_assign,

        'agent' AS actor_type,
        'msg' AS action_type,

        m.created_at AS action_created_at,
        LAG(m.created_at) OVER (
            PARTITION BY a.ticket_id
            ORDER BY m.created_at
        ) AS last_prev_action_created_at,

        m.author_id AS action_author_id,
        m.events__body AS msg_body,

        m.ticket_reply_num,
        m.agent_reply_num

    FROM assign a
    JOIN agent_messages m
      ON a.ticket_id = m.ticket_id
     AND m.created_at >= a.created_at
     AND m.created_at <  a.next_assign


    UNION ALL

    /* ---------- 3. ПУСТЫЕ АССАЙНЫ → to_check ---------- */
    SELECT
        a.ticket_id,
        a.requester_id,
        COALESCE(a.author_id_value, a.author_id) AS assignee_id,
        a.events__id AS assignee_event_id,
        a.created_at AS assign_created_at,
        a.next_assign,

        'agent' AS actor_type,
        'to_check' AS action_type,

        a.next_assign AS action_created_at,
        a.created_at AS last_prev_action_created_at,

        NULL AS action_author_id,
        NULL AS msg_body,

        NULL AS ticket_reply_num,
        NULL AS agent_reply_num

    FROM assign a
    WHERE 1=1
      AND COALESCE(a.author_id_value, a.author_id) <> a.requester_id
      AND NOT EXISTS (
        SELECT 1
        FROM agent_messages m
        WHERE m.ticket_id = a.ticket_id
          AND m.created_at >= a.created_at
          AND m.created_at <  a.next_assign
    )
),
    final_log AS (
SELECT
    ticket_id,
    requester_id,
    assignee_id,

    /* ---------- actor_type с auto ---------- */
    CASE
        WHEN action_author_id = 26440502459665 THEN 'auto'
        ELSE actor_type
    END AS actor_type,

    action_type,
    ticket_reply_num,
    agent_reply_num,

    action_started_at,
    action_finished_at,
    action_duration_sec,

    action_author_id,
    msg_body,

    /* ---------- флаг короткого to_check ---------- */
    CASE
        WHEN action_type = 'to_check'
         AND action_duration_sec < 300
        THEN action_duration_sec
        ELSE 0
    END AS short_to_check_sec
FROM (
    SELECT
        ticket_id,
        requester_id,
        assignee_id,
        actor_type,
        action_type,
        ticket_reply_num,
        agent_reply_num,

        COALESCE(
            LAG(action_created_at) OVER (
                PARTITION BY ticket_id
                ORDER BY action_created_at
            ),
            action_created_at
        ) AS action_started_at,

        action_created_at AS action_finished_at,

        COALESCE(
            DATE_DIFF(
                'second',
                LAG(action_created_at) OVER (
                    PARTITION BY ticket_id
                    ORDER BY action_created_at
                ),
                action_created_at
            ),
            0
        ) AS action_duration_sec,

        action_author_id,
        msg_body
    FROM log_raw
    )
),
    ready_to_agg AS (
SELECT
    ticket_id,
    requester_id,
    assignee_id,
    action_started_at,
    action_finished_at,

    /* ---------- перераспределение длительности ---------- */
    COALESCE(
    CASE
        WHEN action_type = 'to_check'
         AND action_duration_sec < 300
        THEN 0
        ELSE action_duration_sec
          + LAG(short_to_check_sec) OVER (
                PARTITION BY ticket_id
                ORDER BY action_finished_at
            )
    END
    , 0) AS action_duration_sec,
    actor_type,
    action_type,
    ticket_reply_num,
    agent_reply_num,
    msg_body
FROM final_log
ORDER BY ticket_id, action_finished_at
)

SELECT ticket_id,
       MIN(action_started_at) as started_at,
       MAX(action_finished_at) as finished_at,
       DATE_DIFF('second', MIN(action_started_at), MAX(action_finished_at)) AS total_resolution_time,
       (
        DATE_DIFF('second', MIN(action_started_at), MAX(action_finished_at))  -
        SUM(CASE WHEN actor_type = 'customer' THEN action_duration_sec ELSE 0 END)
       ) as handling_time,
       SUM(CASE WHEN action_type = 'to_check' THEN action_duration_sec ELSE 0 END) as to_check_time,
       SUM(CASE WHEN ticket_reply_num = 1 THEN action_duration_sec ELSE 0 END) as first_rt,
       SUM(CASE WHEN ticket_reply_num = 2 THEN action_duration_sec ELSE 0 END) as second_rt,
       AVG(CASE WHEN ticket_reply_num <> 1 THEN action_duration_sec ELSE 0 END) as consecutive_rt,
       COUNT(DISTINCT assignee_id) - 1 as total_assignees_cnt,
       COUNT(DISTINCT CASE WHEN ticket_reply_num is not null THEN assignee_id END) as active_assignees_cnt,
       SUM(CASE WHEN actor_type <> 'customer' AND action_type = 'msg' THEN 1 ELSE 0 END) as replies_cnt
FROM ready_to_agg
GROUP BY ticket_id

;
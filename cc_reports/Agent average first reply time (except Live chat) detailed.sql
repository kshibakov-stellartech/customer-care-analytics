SELECT
  ticket_id AS ticket_id,
  ticket_created_date AS ticket_created_date,
  start_timestamp AS start_timestamp,
  end_timestamp AS end_timestamp,
  reply_time_minutes AS reply_time_minutes,
  agent_name AS agent_name
FROM (
  WITH events AS (
    SELECT
      ticket_id,
      event_at,
      event_type,
      event_value,
      ticket_created_at,
      CAST(event_author_id AS BIGINT) AS event_author_id
    FROM data_silver_zendesk_prod.zendesk_events
    WHERE
      ticket_created_at >= CAST('2025-01-01 00:00:00' AS TIMESTAMP)
  ), chat_started_tickets AS (
    SELECT DISTINCT
      ticket_id
    FROM events
    WHERE
      event_type = 'ChatStartedEvent'
  ), bot_first_reply AS (
    SELECT
      ticket_id,
      MIN(event_at) AS bot_first_reply_at
    FROM events
    WHERE
      event_type = 'reply_by_agent' AND event_author_id = 26440502459665
    GROUP BY
      ticket_id
  ), human_first_reply AS (
    SELECT
      ticket_id,
      MIN(event_at) AS human_first_reply_at
    FROM events
    WHERE
      event_type = 'reply_by_agent' AND event_author_id <> 26440502459665
    GROUP BY
      ticket_id
  ), human_replies_ranked AS (
    SELECT
      ticket_id,
      event_at,
      ROW_NUMBER() OVER (PARTITION BY ticket_id ORDER BY event_at) AS human_reply_rn
    FROM events
    WHERE
      event_type = 'reply_by_agent' AND event_author_id <> 26440502459665
  ), second_human_reply AS (
    SELECT
      ticket_id,
      event_at AS second_human_reply_at
    FROM human_replies_ranked
    WHERE
      human_reply_rn = 2
  ), first_customer_reply AS (
    SELECT
      ticket_id,
      MIN(event_at) AS first_customer_reply_at
    FROM events
    WHERE
      event_type = 'reply_by_customer'
    GROUP BY
      ticket_id
  ), ticket_created AS (
    SELECT
      ticket_id,
      MIN(ticket_created_at) AS ticket_created_at
    FROM events
    GROUP BY
      ticket_id
  ), conversation_bounds AS (
    SELECT
      h.ticket_id,
      CASE
        WHEN NOT c.first_customer_reply_at IS NULL
        AND c.first_customer_reply_at < h.human_first_reply_at
        THEN c.first_customer_reply_at
        ELSE h.human_first_reply_at
      END AS user_first_message_at,
      CASE
        WHEN NOT c.first_customer_reply_at IS NULL
        AND c.first_customer_reply_at < h.human_first_reply_at
        THEN h.human_first_reply_at
        ELSE sh.second_human_reply_at
      END AS agent_first_message_at
    FROM human_first_reply AS h
    LEFT JOIN first_customer_reply AS c
      ON h.ticket_id = c.ticket_id
    LEFT JOIN second_human_reply AS sh
      ON h.ticket_id = sh.ticket_id
  ), automation_before_agent AS (
    SELECT DISTINCT
      e.ticket_id
    FROM events AS e
    JOIN conversation_bounds AS cb
      ON e.ticket_id = cb.ticket_id
    WHERE
      e.event_type = 'reply_by_automation'
      AND e.event_at < cb.agent_first_message_at
      AND (
        e.event_value IS NULL
        OR NOT e.event_value LIKE 'Automated notification: Auto_1: Initial Notification%'
      )
  ), ticket_last_tags AS (
    SELECT
      ticket_id,
      MAX_BY(event_value, event_at) AS last_tags
    FROM events
    WHERE
      event_type = 'ticket_tag_change'
    GROUP BY
      ticket_id
  ), tickets_with_excluded_tags AS (
    SELECT
      ticket_id
    FROM ticket_last_tags
    WHERE
      last_tags LIKE '%cancellation_notification%'
      OR last_tags LIKE '%closed_by_merge%'
      OR last_tags LIKE '%voice_abandoned_in_voicemail%'
      OR last_tags LIKE '%appfollow%'
      OR last_tags LIKE '%spam%'
      OR last_tags LIKE '%ai_cb_triggered%'
      OR last_tags LIKE '%chargeback_precom%'
      OR last_tags LIKE '%chargeback_postcom%'
  ), valid_first_reply AS (
    SELECT
      cb.ticket_id,
      cb.user_first_message_at AS start_timestamp,
      cb.agent_first_message_at AS end_timestamp
    FROM conversation_bounds AS cb
    JOIN ticket_created AS t
      ON cb.ticket_id = t.ticket_id
    LEFT JOIN bot_first_reply AS b
      ON cb.ticket_id = b.ticket_id
    WHERE
      NOT cb.user_first_message_at IS NULL
      AND NOT cb.agent_first_message_at IS NULL
      AND cb.user_first_message_at >= t.ticket_created_at
      AND cb.agent_first_message_at > cb.user_first_message_at
      AND (
        b.bot_first_reply_at IS NULL OR cb.agent_first_message_at < b.bot_first_reply_at
      )
      AND NOT cb.ticket_id IN (
        SELECT
          ticket_id
        FROM chat_started_tickets
      )
      AND NOT cb.ticket_id IN (
        SELECT
          ticket_id
        FROM automation_before_agent
      )
      AND NOT cb.ticket_id IN (
        SELECT
          ticket_id
        FROM tickets_with_excluded_tags
      )
  ), final_frt AS (
    SELECT
      v.ticket_id,
      v.start_timestamp,
      v.end_timestamp,
      DATE_DIFF('MINUTE', v.start_timestamp, v.end_timestamp) AS reply_time_minutes,
      CAST(v.start_timestamp AS DATE) AS ticket_created_date,
      CASE
        WHEN e.event_author_id = 26349132549521
        THEN 'Aleksandra Petchenko'
        WHEN e.event_author_id = 30160506886161
        THEN 'Alex Poponin'
        WHEN e.event_author_id = 30648746936465
        THEN 'Alexander Petrov'
        WHEN e.event_author_id = 38657563018769
        THEN 'Alisa Sakharova'
        WHEN e.event_author_id = 38022764826129
        THEN 'Alyona Kostukovich'
        WHEN e.event_author_id = 35310711957393
        THEN 'Ani Monaselidze'
        WHEN e.event_author_id = 38754864964753
        THEN 'Arsenii Tepliuk'
        WHEN e.event_author_id = 29737848444689
        THEN 'Artem Vinokurov'
        WHEN e.event_author_id = 33118701264017
        THEN 'Daria Saranchova'
        WHEN e.event_author_id = 38022759246737
        THEN 'Ekaterina Rumiantseva'
        WHEN e.event_author_id = 37992873903889
        THEN 'Hanna Dereka'
        WHEN e.event_author_id = 30786139608081
        THEN 'Kateryna Kasper'
        WHEN e.event_author_id = 30655366698001
        THEN 'Kateryna Moroz'
        WHEN e.event_author_id = 33118711659921
        THEN 'Kateryna Novikova'
        WHEN e.event_author_id = 26222438547857
        THEN 'Maksym Zvieriev'
        WHEN e.event_author_id = 38694917174545
        THEN 'Mher Mkrtumyan'
        WHEN e.event_author_id = 36064560830737
        THEN 'Mykyta'
        WHEN e.event_author_id = 26440502459665
        THEN 'Nikki'
        WHEN e.event_author_id = 39272670052113
        THEN 'Oleksii Bondar'
        WHEN e.event_author_id = 31467436910865
        THEN 'Yekaterina Popivnukhina'
        WHEN e.event_author_id = 33602186941713
        THEN 'Yuliia Si'
        ELSE 'unknown'
      END AS agent_name
    FROM valid_first_reply AS v
    LEFT JOIN events AS e
      ON v.ticket_id = e.ticket_id
      AND v.end_timestamp = e.event_at
      AND e.event_type = 'reply_by_agent'
      AND e.event_author_id <> 26440502459665
  )
  SELECT
    ticket_id,
    ticket_created_date,
    start_timestamp,
    end_timestamp,
    reply_time_minutes,
    agent_name
  FROM final_frt
) AS virtual_table
WHERE
  ticket_created_date >= CAST('2025-11-18' AS DATE)
  AND ticket_created_date < CAST('2025-11-25' AS DATE)
LIMIT 50000;
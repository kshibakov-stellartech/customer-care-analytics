SELECT
  ticket_id AS ticket_id,
  ticket_created_date AS ticket_created_date,
  start_timestamp AS start_timestamp,
  end_timestamp AS end_timestamp,
  reply_time_minutes AS reply_time_minutes
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
  ), chat_started_tickets /* Тикеты с ChatStartedEvent → исключаем */ AS (
    SELECT DISTINCT
      ticket_id
    FROM events
    WHERE
      event_type = 'ChatStartedEvent'
  ), ticket_created /* Время создания тикета */ AS (
    SELECT
      ticket_id,
      MIN(ticket_created_at) AS ticket_created_at
    FROM events
    GROUP BY
      ticket_id
  ), customer_messages /* Все клиентские сообщения (reply_by_customer) с порядком */ AS (
    SELECT
      ticket_id,
      event_at,
      ROW_NUMBER() OVER (PARTITION BY ticket_id ORDER BY event_at) AS customer_seq
    FROM events
    WHERE
      event_type = 'reply_by_customer'
  ), agent_at_created /* Агентские ответы в момент создания тикета (для правила №2) */ AS (
    SELECT DISTINCT
      e.ticket_id
    FROM events AS e
    JOIN ticket_created AS t
      ON e.ticket_id = t.ticket_id
    WHERE
      e.event_type = 'reply_by_agent' AND e.event_at = t.ticket_created_at
  ), customer_points /* Собираем first/second reply_by_customer + флаг "есть reply_by_agent в момент создания" */ AS (
    SELECT
      t.ticket_id,
      t.ticket_created_at,
      MIN(CASE WHEN cm.customer_seq = 1 THEN cm.event_at END) AS first_customer_at,
      MIN(CASE WHEN cm.customer_seq = 2 THEN cm.event_at END) AS second_customer_at,
      CASE WHEN NOT a.ticket_id IS NULL THEN 1 ELSE 0 END AS has_agent_at_created
    FROM ticket_created AS t
    LEFT JOIN customer_messages AS cm
      ON t.ticket_id = cm.ticket_id
    LEFT JOIN agent_at_created AS a
      ON t.ticket_id = a.ticket_id
    GROUP BY
      t.ticket_id,
      t.ticket_created_at,
      CASE WHEN NOT a.ticket_id IS NULL THEN 1 ELSE 0 END
  ), second_customer_point /* Выбираем "второе сообщение клиента" с учётом правил: */ /* 1) если есть reply_by_agent в момент создания → берём ПЕРВЫЙ reply_by_customer */ /* 2) если первый reply_by_customer = ticket_created_at → берём ВТОРОЙ reply_by_customer */ /* 3) во всех остальных случаях — берём ВТОРОЙ reply_by_customer (классическая логика) */ AS (
    SELECT
      ticket_id,
      ticket_created_at,
      CASE
        WHEN has_agent_at_created = 1
        THEN first_customer_at /* правило №2 */
        WHEN first_customer_at = ticket_created_at
        THEN second_customer_at /* правило №1 */
        ELSE second_customer_at /* дефолт: второе сообщение */
      END AS second_customer_at
    FROM customer_points
  ), human_agent_replies /* Все ответы живых агентов (без бота) */ AS (
    SELECT
      ticket_id,
      event_at AS agent_reply_at
    FROM events
    WHERE
      event_type = 'reply_by_agent' AND event_author_id <> 26440502459665
  ), first_human_after_second /* Первый ответ живого агента ПОСЛЕ выбранного "второго сообщения клиента" */ AS (
    SELECT
      s.ticket_id,
      s.ticket_created_at,
      s.second_customer_at,
      MIN(h.agent_reply_at) AS first_human_reply_at
    FROM second_customer_point AS s
    JOIN human_agent_replies AS h
      ON s.ticket_id = h.ticket_id AND h.agent_reply_at > s.second_customer_at
    WHERE
      NOT s.second_customer_at IS NULL /* должны иметь валидное "второе" сообщение */
    GROUP BY
      s.ticket_id,
      s.ticket_created_at,
      s.second_customer_at
  ), bot_between_second_and_human /* Проверка: нет ли бота между "вторым сообщением клиента" и первым живым агентом */ AS (
    SELECT DISTINCT
      e.ticket_id
    FROM events AS e
    JOIN second_customer_point AS s
      ON e.ticket_id = s.ticket_id
    JOIN first_human_after_second AS h
      ON e.ticket_id = h.ticket_id
    WHERE
      e.event_type = 'reply_by_agent'
      AND e.event_author_id = 26440502459665 /* бот */
      AND e.event_at > s.second_customer_at
      AND e.event_at < h.first_human_reply_at
  ), second_message_frt /* Валидные интервалы FRT для ответа на "второе" сообщение клиента */ AS (
    SELECT
      t.ticket_id,
      t.ticket_created_at,
      s.second_customer_at AS start_timestamp,
      h.first_human_reply_at AS end_timestamp
    FROM first_human_after_second AS h
    JOIN second_customer_point AS s
      ON h.ticket_id = s.ticket_id
    JOIN ticket_created AS t
      ON h.ticket_id = t.ticket_id
    LEFT JOIN bot_between_second_and_human AS b
      ON h.ticket_id = b.ticket_id
    WHERE
      h.first_human_reply_at > s.second_customer_at
      AND NOT h.ticket_id IN (
        SELECT
          ticket_id
        FROM chat_started_tickets
      )
      AND b.ticket_id IS NULL /* если не хотим бота между 2-м юзерским и агентом */
  ), final_frt AS (
    SELECT
      ticket_id,
      ticket_created_at,
      start_timestamp,
      end_timestamp,
      DATE_DIFF('MINUTE', start_timestamp, end_timestamp) AS reply_time_minutes,
      CAST(ticket_created_at AS DATE) AS ticket_created_date
    FROM second_message_frt
    WHERE
      end_timestamp > start_timestamp
  )
  SELECT
    ticket_id,
    ticket_created_date,
    ticket_created_at,
    start_timestamp,
    end_timestamp,
    reply_time_minutes
  FROM final_frt
) AS virtual_table
WHERE
  ticket_created_date >= CAST('2025-11-10' AS DATE)
  AND ticket_created_date < CAST('2025-11-19' AS DATE)
LIMIT 50000;
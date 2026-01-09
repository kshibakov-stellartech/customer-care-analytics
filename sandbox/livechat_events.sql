WITH
    row_data AS (
SELECT ticket_id,
     created_at,
     ticket_updated_date,
     ticket_updated_at,
     events__id,
     channel,
     author_id,
     events__author_id,
     events__type,
     events__field_name,
     events__value,
     events__previous_value,
     events__body,
     events__public,
     events__channel,
     events__subject,
     events__from_title
FROM data_bronze_zendesk_prod.zendesk_audit
WHERE 1 = 1
AND ticket_id = 611138
AND (
  events__type IN (
                   'ChatStartedEvent',
                   'ChatEndedEvent',
                   'Comment')
      OR
  events__field_name IN ('status', 'assignee_id')
  )
--LIMIT 10
),
    chat_frames AS (
SELECT ticket_id,
       created_at,
       created_at as finished_at,
       events__type,
       lag(created_at, 1) over(partition by ticket_id order by created_at, events__id) as started_at
FROM row_data
WHERE 1=1
  AND events__type IN (
                   'ChatStartedEvent',
                   'ChatEndedEvent'
                      )
ORDER BY created_at, events__id
),
    split_data AS (
SELECT
    row_data.ticket_id,
    rd2.events__value as assignee_id,
    started_at,
    finished_at,
    -- Разрезаем строку по паттерну времени в скобках, но используем lookahead,
    -- чтобы не удалять само время из текста (если ваш диалект поддерживает это)
    -- В Athena проще разрезать и затем очистить пустые значения
    regexp_split(row_data.events__body, '\n(?=\(\d{2}:\d{2}:\d{2}\))') as messages
FROM row_data
    left join chat_frames using(created_at)
    left join row_data rd2 ON rd2.created_at >= chat_frames.started_at
                          AND rd2.created_at <= chat_frames.finished_at
                          AND rd2.events__field_name = 'assignee_id'
WHERE 1=1
  AND row_data.events__type = 'Comment'
  AND row_data.channel = 'chat_transcript'
),
    expanded_messages AS (
SELECT
    ticket_id,
    assignee_id,
    started_at,
    finished_at,
    msg AS full_message_line
FROM split_data
CROSS JOIN UNNEST(messages) AS t(msg)
WHERE 1=1
  AND msg != ''
  AND assignee_id is not null
)


SELECT
    ticket_id,
    assignee_id,
    started_at,
    finished_at,
    -- Извлекаем время: (13:31:52)
    regexp_extract(full_message_line, '^\(\d{2}:\d{2}:\d{2}\)') as msg_time,
    -- Извлекаем автора: текст после времени до первого двоеточия
    regexp_extract(full_message_line, '^\(\d{2}:\d{2}:\d{2}\)\s+([^:]+):', 1) as author,
    -- Извлекаем сам текст сообщения: все что после автора и двоеточия
    regexp_replace(full_message_line, '^\(\d{2}:\d{2}:\d{2}\)\s+[^:]+:\s*', '') as message_text
FROM expanded_messages
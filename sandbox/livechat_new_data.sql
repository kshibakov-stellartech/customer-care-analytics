WITH
    tbl AS (
SELECT
      ticket_id,
      events__chat_started.channel,
      events__chat_started.chat_id,
      events__chat_started.conversation_id,
      events__chat_started.end_user_email,
      events__chat_started.agent_id,
      events__chat_started.agent_name,
      timeline_entry.idx,
      timeline_entry.type,
      timeline_entry.actor_id,
      timeline_entry.timestamp,
      timeline_entry.content_type,
      timeline_entry.content_text,
      events__chat_started.outcome.issue_type,
      events__chat_started.outcome.agent_actions,
      events__chat_started.outcome.resolution.status AS
  resolution_status,
      events__chat_started.outcome.resolution.cancelled AS
  resolution_cancelled,
      events__chat_started.outcome.resolution.retention_offer AS
  resolution_retention_offer
  FROM data_bronze_zendesk_stage.zendesk_audit
    LEFT JOIN UNNEST(events__chat_started.timeline) AS t(timeline_entry) ON TRUE
  WHERE events__type = 'ChatStartedEvent'
      AND events__chat_started IS NOT NULL
      AND ticket_id = 627702
),
    ticket_info AS (
SELECT ticket_id,
       created_at,
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
WHERE ticket_id = 627702--694785
/*  AND events__type IN (
                        'Comment'
                      )*/
  --AND events__field_name = '40831328206865'
  /*
  AND events__type = 'ChatStartedEvent'
  */
ORDER BY created_at, events__id
),

    full_conv AS (
SELECT ticket_id,
       created_at,
       CAST(author_id AS VARCHAR) as author_id,
       channel,
       events__body
FROM ticket_info
WHERE 1=1
  AND events__type = 'Comment'
  AND channel <> 'chat_transcript'
  AND author_id <> -1
UNION ALL
SELECT ticket_id,
       from_unixtime(CAST(tbl.timestamp AS BIGINT) / 1000.0) as created_at,
       CAST(actor_id AS VARCHAR) as author_id,
       'messaging' as channel,
       content_text as events__body
FROM tbl
)

SELECT *
FROM full_conv
ORDER BY created_at
;
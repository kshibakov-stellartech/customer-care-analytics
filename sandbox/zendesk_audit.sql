WITH ticket_info AS (
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
       events__type,
       events__channel,
       events__subject,
       events__from_title
FROM data_bronze_zendesk_prod.zendesk_audit
WHERE ticket_id = 575954
/*  AND events__type IN (
                        'Comment'
                      )*/
  --AND events__field_name = '40831328206865'
  /*
  AND events__type = 'ChatStartedEvent'
  */
ORDER BY created_at, events__id
)

SELECT *
FROM ticket_info
WHERE 1=1


/*
31320582354705
*/

;

WITH ticket_info AS (
SELECT ticket_id,
       created_at,
       events__id,
       channel,
       events__type,
       events__value
FROM data_bronze_zendesk_prod.zendesk_audit
WHERE 1=1
  AND ticket_id = 601145
  AND events__type = 'ChatStartedEvent'
ORDER BY created_at, events__id
)

SELECT *
FROM ticket_info
WHERE 1=1


/*
31320582354705
*/
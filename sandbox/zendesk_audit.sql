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
WHERE ticket_id = 617149
/*  AND events__type IN (
                        'Comment'
                      )*/
  --AND events__field_name = '40831328206865'
  /*
  AND events__type IN (
                        --'ChatStartedEvent'
                        --,'ChatEndedEvent'
                      )
  */
ORDER BY created_at, events__id
--LIMIT 10
;

SELECT *
FROM (
SELECT ticket_id,
       created_at,
       ticket_updated_date,
       ticket_updated_at,
       events__field_name,
       events__type,
       events__value,
       events__previous_value,
       LAG(created_at, 1) over(PARTITION BY ticket_id ORDER BY created_at) as prev_action,
       DATE_DIFF('second', LAG(created_at, 1) over(PARTITION BY ticket_id ORDER BY created_at), created_at) as proccessing_time
FROM data_bronze_zendesk_prod.zendesk_audit za
WHERE ticket_id = 617149
  AND events__field_name = 'assignee_id'
UNION ALL
SELECT ticket_id,
       created_at,
       ticket_updated_date,
       ticket_updated_at,
       events__field_name,
       events__type,
       events__value,
       events__previous_value,
       LAG(created_at, 1) over(PARTITION BY ticket_id ORDER BY created_at) as prev_action,
       DATE_DIFF('second', LAG(created_at, 1) over(PARTITION BY ticket_id ORDER BY created_at), created_at) as proccessing_time
FROM data_bronze_zendesk_prod.zendesk_audit za
WHERE ticket_id = 593604
  AND events__field_name = 'status'
) q
ORDER BY created_at

;
/*

*/


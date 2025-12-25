SELECT *
FROM data_silver_zendesk_prod.zendesk_events
WHERE 1=1
  --AND event_at >= DATE '2025-11-01'
  AND ticket_id = 629071
  --AND (lower(event_type) LIKE '%chat%' OR lower(event_type) LIKE '%status%' OR lower(event_type) LIKE '%reply%')
  --AND event_type = 'status_change'
ORDER BY event_at
;

SELECT ticket_id,
       created_at,
       events__type,
       events__field_name,
       typeof(events__value) AS value_type,
       events__value
FROM data_bronze_zendesk_prod.zendesk_audit
WHERE ticket_id = 629071
  --AND events__field_name = '40831328206865'
  /*
  AND events__type IN (
                        --'ChatStartedEvent'
                        --,'ChatEndedEvent'
                      )
  */
ORDER BY created_at
--LIMIT 10
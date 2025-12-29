SELECT *
FROM data_bronze_zendesk_prod.zendesk_audit
WHERE ticket_id = 593604
  --AND events__field_name = '40831328206865'
  /*
  AND events__type IN (
                        --'ChatStartedEvent'
                        --,'ChatEndedEvent'
                      )
  */
ORDER BY created_at
--LIMIT 10
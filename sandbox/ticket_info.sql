WITH
    ticket_attr AS (
SELECT ticket_id,
       MAX(CASE WHEN events__type = 'Create' AND events__field_name = 'requester_id' THEN events__value END) as requester_id,
       MAX(CASE WHEN events__field_name IN (
                                             '32351109113361', /* backoffice */
                                             '40831328206865', /* app_user_id */
                                             '32351085497873' /* supabase */
                                            )
                                        THEN events__value END
       ) as user_id,
       MAX(CASE WHEN events__type = 'Create' AND events__field_name = 'brand_id' THEN
               CASE WHEN events__value = '26467992035601' THEN 'MindScape'
                    WHEN events__value = '27810244289553' THEN 'Neurolift'
                    WHEN events__value = '26468032413713' THEN 'SmartyMe'
                    WHEN events__value = '26222456156689' THEN 'StellarTech Limited'
                    ELSE 'Unknown'
                    END
           END) as ticket_brand,
       MAX(CASE WHEN events__type = 'Create' AND events__field_name = 'ticket_form_id' THEN
               CASE WHEN events__value = '26472204214801' THEN 'main ticket form'
                    WHEN events__value = '34833592831505' THEN 'in-app ticket form'
                    WHEN events__value = '34902185196177' THEN 'test form'
                    WHEN events__value = '26222488220945' THEN 'default ticket form'
                    WHEN events__value = '35743604923281' THEN 'registration form'
                    ELSE null
                    END
           END) as ticket_form_type,
       MAX(CASE WHEN events__type = 'Create' AND events__field_name = 'requester_id' THEN channel END) as ticket_channel
FROM data_bronze_zendesk_prod.zendesk_audit
WHERE 1=1
  AND ticket_updated_date >= DATE '2025-11-01'
  AND ticket_updated_date <  DATE '2025-12-01'
GROUP BY 1
)

SELECT *
FROM ticket_attr

/*
26472204214801 - main ticket form
34833592831505 - in-app ticket form
34902185196177 - test form
26222488220945 - default ticket form
35743604923281 - registration form
*/
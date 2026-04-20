/*
https://app.asana.com/1/1206371557234276/project/1209882467788483/task/1213961925948701?focus=true
*/
WITH
    tickets_to_exclude AS (
SELECT ticket_id as ticket_to_exclude_id, MIN(CAST(created_at AS DATE)) as created_date
FROM data_bronze_zendesk_prod.zendesk_audit
WHERE 1=1
    AND created_at >= DATE '2024-01-01'
    AND events__field_name = 'tags'
    AND (
       events__value LIKE '%cancellation_notification%'
    OR events__value LIKE '%closed_by_merge%'
    OR events__value LIKE '%voice_abandoned_in_voicemail%'
    OR events__value LIKE '%appfollow%'
    OR events__value LIKE '%spam%'
    OR events__value LIKE '%ai_cb_triggered%'
    OR events__value LIKE '%chargeback_precom%'
    OR events__value LIKE '%chargeback_postcom%'
    )
GROUP BY 1

),
    tickets AS (
SELECT
    ticket_id,
    MIN(created_at) AS ticket_created_at,
    CAST(MAX(events__value) AS BIGINT) AS requester_id
FROM data_bronze_zendesk_prod.zendesk_audit
WHERE events__type = 'Create'
  AND events__field_name = 'requester_id'
GROUP BY ticket_id
HAVING MIN(CAST(created_at AS DATE)) >= DATE '2024-01-01'
   AND MIN(CAST(created_at AS DATE)) < current_date
),
    base_audit AS (
SELECT
    za.ticket_id,
    za.channel,
    date_add('hour', 2, za.created_at) as created_at,
    date_trunc('minute', date_add('hour', 2, za.created_at)) as created_at_truncated,
    CAST(CAST(za.author_id AS DOUBLE) AS BIGINT) AS author_id,
    CAST(CAST(za.events__author_id AS DOUBLE) AS BIGINT) AS event_author_id,
    za.events__id,
    za.events__type,
    za.events__field_name,
    za.events__value,
    za.events__previous_value,
    za.events__body,
    za.events__public,
    za.events__from_title,
    CASE WHEN events__type = 'Notification' AND events__from_title IN (
                                                                        'Auto_12: Auto-reply to refund requests (Stores)',
                                                                        'Auto_21: Auto-reply to delete+refund requests (Paddle/PayPal)',
                                                                        'Auto_91: Auto-reply to delete requests (Stores)',
                                                                        'Auto_13: Auto-reply to refund requests (Paddle/PayPal)',
                                                                        'Auto_29: Auto-reply - payment_not_found AI',
                                                                        'Auto_29: Auto-reply - payment_not_found AI (2nd)',
                                                                        'Auto_29: Auto-reply - payment_not_found (automation failed)',
                                                                        'Auto_35: Auto-reply to delete+refund requests (threats/risk)',
                                                                        'Auto_6: Auto-reply to cancel requests (Web) ',
                                                                        'Auto_7: Auto-reply to cancel requests (Stores)',
                                                                        'Auto_28: Freemium only - payment_not_found',
                                                                        'Auto-reply - something is wrong with my subscription - SmartyMe'
                                                                      )
            THEN 1 /* auto notification */
         WHEN CAST(CAST(za.events__author_id AS DOUBLE) AS BIGINT) is not null AND events__public = TRUE
            THEN 2 /* public message */
            ELSE 0
    END is_public_communication
FROM data_bronze_zendesk_prod.zendesk_audit za
    JOIN tickets ON tickets.ticket_id = za.ticket_id
    LEFT JOIN tickets_to_exclude ON tickets_to_exclude.ticket_to_exclude_id = za.ticket_id
WHERE 1=1
  --AND za.ticket_id = 660296
  AND tickets_to_exclude.ticket_to_exclude_id IS NULL
),

    ticket_attr AS (
SELECT ticket_id,
       MIN(created_at) as ticket_created_at,
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
                    WHEN events__value = '43023476289553' THEN 'Nexera'
                    ELSE 'Unknown'
                    END
           END) as ticket_brand,
       MAX(CASE WHEN events__field_name = '28705421240977'THEN events__value END) as main_sub,
       MAX(CASE WHEN events__field_name = '30974051377041'THEN events__value END) as main_sub_dt_start,
       MAX(CASE WHEN events__field_name = '30971629182737'THEN events__value END) as upsale_sub,
       MAX(CASE WHEN events__field_name = '30971634663185'THEN events__value END) as upsale_sub_dt_start
FROM base_audit
WHERE 1=1
GROUP BY 1
),
    sub_info AS (
SELECT profile_id as user_id,
       CASE WHEN  country = 'AE'
           THEN 'UAE' ELSE 'other'
       END as is_uae,
       project_name,
       MIN(onboarding_started_at) as onboarding_started_at,
       MIN(subscription_created_at) AS sub_created_date
FROM data_silver_product_sessions_prod.sf_purchase_sessions
WHERE 1=1
  AND onboarding_started_at >= DATE '2024-01-01'
GROUP BY 1, 2, 3
),
    joined_data AS (
SELECT sub_info.user_id,
       sub_info.project_name,
       sub_info.onboarding_started_at,
       sub_info.is_uae,
       CASE WHEN sub_info.sub_created_date IS NOT NULL THEN 1 ELSE 0 END as is_sub_exsist,
       ticket_id,
       ticket_brand
FROM sub_info
    LEFT JOIN ticket_attr ON sub_info.user_id = ticket_attr.user_id
WHERE 1=1
),
    result AS (
SELECT CAST(DATE_TRUNC('month', onboarding_started_at) as DATE) as month_dt,
       is_uae,
       COUNT(DISTINCT ticket_id) as tickets_cnt,
       COUNT(CASE WHEN ticket_id IS NOT NULL THEN user_id END) as users_with_ticket
FROM joined_data
WHERE 1=1
  AND project_name = 'smartyme'
GROUP BY 1, 2
ORDER BY 1, 2
)

SELECT *
FROM result
;
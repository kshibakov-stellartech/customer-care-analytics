WITH
    tickets_to_exclude AS (
SELECT ticket_id as ticket_to_exclude_id, MIN(CAST(created_at AS DATE)) as created_date
FROM data_bronze_zendesk_prod.zendesk_audit
WHERE 1=1
    AND created_at >= DATE '2026-01-01'
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
HAVING MIN(CAST(created_at AS DATE)) >= DATE '2026-01-01'
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
       MAX(CASE WHEN events__field_name = '28705421240977'THEN events__value END) as main_sub,
       MAX(CASE WHEN events__field_name = '30974051377041'THEN events__value END) as main_sub_dt_start,
       MAX(CASE WHEN events__field_name = '30971629182737'THEN events__value END) as upsale_sub,
       MAX(CASE WHEN events__field_name = '30971634663185'THEN events__value END) as upsale_sub_dt_start,
       ELEMENT_AT(
           ARRAY_AGG(events__value ORDER BY created_at DESC, events__id DESC)
           FILTER (WHERE events__field_name = '31320582354705' AND events__value IS NOT NULL), 1
       ) as subtype,
       MAX(CASE WHEN events__field_name = 'tags' AND events__value LIKE '%refund_not_eligible%' THEN 1 ELSE 0 END) as refund_not_eligible,
       MAX(CASE WHEN events__field_name = 'tags' AND events__value LIKE '%duplicate_charge%' THEN 1 ELSE 0 END) as double_charged,
       MAX(CASE WHEN events__field_name = 'tags' AND events__value LIKE '%purchased_twice%' THEN 1 ELSE 0 END) as purchased_twice,
       MAX(CASE WHEN events__field_name = 'tags' AND events__value LIKE '%local_legislation%' THEN 1 ELSE 0 END) as local_legal,

       MAX(CASE WHEN events__field_name = 'tags' AND events__value LIKE '%money_back_guarantee%' THEN 1 ELSE 0 END) as money_back_guarantee,
       MAX(CASE WHEN events__field_name = 'tags' AND events__value LIKE '%terms_of_usage%' THEN 1 ELSE 0 END) as terms_of_usage,

       MAX(CASE WHEN events__field_name = 'tags' AND events__value LIKE '%refund_eligible%'     THEN 1 ELSE 0 END) as refund_eligible
--money_back_guarantee
--terms_of_usage
FROM base_audit
WHERE 1=1
GROUP BY 1
),
    sub_info AS (
SELECT profile_id as user_id,
       CASE WHEN  country IN (
                         'BE','ES','HU','SK',
                         'BG','FR','MT','FI',
                         'CZ','HR','NL','SE',
                         'DK','IT','AT','DE',
                         'CY','PL','IS','EE',
                         'LV','PT','LI','IE',
                         'LT','RO','NO','EL',
                         'LU','SI')
           THEN 'EEA' ELSE 'other'
       END as is_EEA,
       MIN(subscription_created_at) AS sub_created_date,
       MIN(subscription_canceled_at) AS sub_cancelled_date,
       SUM(price_usd) as price_usd
FROM data_silver_product_sessions_prod.sf_purchase_sessions
WHERE 1=1
  AND subscription_created_at >= DATE '2026-01-01'
GROUP BY 1, 2
),
    result AS (
SELECT sub_info.user_id,
       sub_info.is_EEA,
       sub_info.sub_created_date,
       sub_info.sub_cancelled_date,
       ticket_id,
       ticket_created_at,
       main_sub,
       main_sub_dt_start,
       upsale_sub,
       upsale_sub_dt_start,
       DATE_DIFF('hour', sub_info.sub_created_date, ticket_created_at) as time_diff,
       CASE WHEN DATE_DIFF('hour', sub_info.sub_created_date, ticket_created_at) <= 24 then 1
       END as ticket_sub_window_1,
       CASE WHEN DATE_DIFF('hour', sub_info.sub_created_date, ticket_created_at) > 24 AND DATE_DIFF('hour', sub_info.sub_created_date, ticket_created_at) <= 336 then 1
       END as ticket_sub_window_14,
       CASE WHEN DATE_DIFF('hour', sub_info.sub_created_date, ticket_created_at) > 336 then 1
       END as ticket_sub_window_other,
       subtype,
       refund_eligible,
       refund_not_eligible,
       double_charged,
       purchased_twice,
       local_legal,
       money_back_guarantee,
       terms_of_usage
FROM sub_info
    JOIN ticket_attr ON sub_info.user_id = ticket_attr.user_id
WHERE 1=1
)

SELECT CAST(DATE_TRUNC('week', ticket_created_at) as DATE) as week_dt,
       ta.refund_eligible,
       ta.refund_not_eligible,
       COUNT(ta.ticket_id) as ticket_cnt,
       COUNT(CASE WHEN csat.rating = 'good' THEN ta.ticket_id END) * 1.0 / COUNT(CASE WHEN csat.rating IN ('good', 'bad') THEN ta.ticket_id END) as csat
FROM ticket_attr ta
    JOIN data_bronze_zendesk_prod.zendesk_csat csat ON ta.ticket_id = csat.ticket_id
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
;
SELECT is_EEA,
       DATE_DIFF('hour', sub_created_date, sub_cancelled_date) as time_diff,
       COUNT(user_id) as users
FROM sub_info
WHERE sub_created_date is not null
GROUP BY 1, 2
ORDER BY 1, 2

;
SELECT --CAST(DATE_TRUNC('week', sub_created_date) as DATE) as week_dt,
       is_EEA,
       CASE WHEN refund_eligible = 1 THEN 'refund_eligible' ELSE 'refund_not_eligible' END as refund_type,
       --double_charged,
       --purchased_twice,
       --local_legal,
       --money_back_guarantee,
       --terms_of_usage,
       COUNT(CASE WHEN DATE_DIFF('hour', sub_created_date, ticket_created_at) <= 24 then user_id END) as ticket_sub_window_1,
       COUNT(CASE WHEN DATE_DIFF('hour', sub_created_date, ticket_created_at) > 24 AND DATE_DIFF('hour', sub_created_date, ticket_created_at) <= 336 then user_id END) as ticket_sub_window_14,
       COUNT(CASE WHEN DATE_DIFF('hour', sub_created_date, ticket_created_at) > 336 then user_id END) as ticket_sub_window_other,
       COUNT(user_id) as all_users
FROM result
WHERE 1=1
  AND (
      refund_eligible = 1 OR
      refund_not_eligible = 1
    )
GROUP BY 1, 2--, 3, 4, 5, 6
ORDER BY 1, 2--, 3, 4, 5, 6
;

SELECT *
FROM result
WHERE 1=1
  AND ticket_sub_window_14 = 1
  AND refund_eligible = 1
  AND double_charged = 0
  AND purchased_twice = 0
  AND local_legal = 0
  AND money_back_guarantee = 0
  AND terms_of_usage = 0
;

SELECT subtype,
       refund_not_eligible,
       refund_eligible,
       COUNT(1) as cnt
FROM ticket_attr
WHERE 1=1
  AND (
      refund_not_eligible = 1 OR refund_eligible = 1
    )
GROUP BY 1, 2, 3
;

SELECT *
FROM data_silver_product_sessions_prod.sf_purchase_sessions
WHERE 1=1
  AND subscription_created_at >= DATE '2026-01-01'
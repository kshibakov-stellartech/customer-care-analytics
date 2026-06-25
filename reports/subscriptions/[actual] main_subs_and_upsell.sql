WITH raw_subs AS (
SELECT project_name,
       user_id,
       purchase_completed_at,
       subscription_created_at,
       current_period_start,
       current_period_end,
       subscription_updated_at,
       subscription_canceled_at,
       vendor,
       vendor_customer_id,
       vendor_subscription_id,
       parent_subscription_id,
       is_recurrent,
       is_upsell,
       cs.status,
       cs.object,
       user_email
FROM data_silver_product_sessions_prod.sf_purchase_sessions ps
    LEFT JOIN chargebee_product_catalog_2.subscription cs ON ps.vendor_subscription_id = cs.id
WHERE 1=1
  AND subscription_created_at >= DATE '2026-01-01'
),
    parent_agg AS (
SELECT parent_subscription_id,
       COUNT(DISTINCT CASE WHEN subscription_type = 'main'
                            AND status IN ('cancelled', 'non-renewing')
                                THEN vendor_subscription_id
       END) as main_cancelled_total,
       COUNT(DISTINCT CASE WHEN subscription_type = 'upsell'
                            AND status IN ('active')
                            AND is_recurrent = true
                                THEN vendor_subscription_id
       END) as upsell_reccurent_active_total,
       COUNT(DISTINCT vendor_subscription_id) as subs_total
FROM (
SELECT *,
       CASE WHEN is_upsell = false THEN 'main' ELSE 'upsell' END AS subscription_type,
       CASE WHEN subscription_canceled_at is not null THEN 1 ELSE 0 END as canceled_flag
FROM raw_subs
WHERE 1=1
) q
GROUP BY 1
),

    subs_to_check AS (
SELECT *
FROM parent_agg
WHERE 1=1
  AND main_cancelled_total = 1
  AND upsell_reccurent_active_total = 1
)

SELECT raw_subs.user_id,
       raw_subs.user_email,
       raw_subs.vendor_customer_id as chargebee_customer_id,
       raw_subs.parent_subscription_id as main_sub_id,
       MAX(CASE WHEN is_upsell = false THEN raw_subs.status END) as main_sub_status,
       MAX(CASE WHEN is_upsell = false THEN subscription_created_at END) as main_sub_created_at,

       MAX(CASE WHEN is_upsell = true THEN vendor_subscription_id END) as upsell_sub_id,
       MAX(CASE WHEN is_upsell = true THEN raw_subs.status END) as upsell_sub_status,
       MAX(CASE WHEN is_upsell = true THEN subscription_created_at END) as upsell_sub_created_at
FROM subs_to_check
    JOIN raw_subs ON subs_to_check.parent_subscription_id = raw_subs.parent_subscription_id
    LEFT JOIN chargebee_product_catalog_2.subscription cs ON raw_subs.vendor_subscription_id = cs.id
GROUP BY 1, 2, 3, 4
;


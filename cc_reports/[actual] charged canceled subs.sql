WITH
params AS (
  SELECT DATE '2025-01-01' AS start_date
),

/* ===== добавлено: источник payment_id и rebill_purchase_completed_at ===== */
payrails_raw AS (
  SELECT
    "time" AS dt,
    details__execution__id,
    element_at(details__payment_composition, 1).payment_id AS payment_id
  FROM firehose_payrails_webhook_prod.payrails
  WHERE "time" >= DATE '2025-01-01'
    AND details__action = 'authorize'
),

transactions_raw AS (
  SELECT
    from_unixtime(ch_tr.date) AS dt,
    ch_tr.subscription_id,
    ch_tr.reference_number,
    pr.payment_id,
    pr.dt AS rebill_purchase_completed_at,
    ROW_NUMBER() OVER (
      PARTITION BY ch_tr.subscription_id
      ORDER BY ch_tr.date DESC
    ) AS rn
  FROM data_bronze_chargebee_prod.transaction ch_tr
  LEFT JOIN payrails_raw pr
    ON ch_tr.reference_number = pr.details__execution__id
  WHERE from_unixtime(ch_tr.date) >= DATE '2025-01-01'
),

latest_transaction_per_subscription AS (
  SELECT
    subscription_id,
    payment_id,
    rebill_purchase_completed_at
  FROM transactions_raw
  WHERE rn = 1
),

-- 1) cancel events (исторически)
cancel_events AS (
  SELECT
    user_id,
    event_time AS cancelation_completed_at,
    json_extract_scalar(CAST(event_properties AS json), '$.planType') AS plan_type
  FROM data_bronze_amplitude_prod.amplitude_resource
  WHERE event_type = 'cancelation_completed'
    AND event_time >= (SELECT start_date FROM params)
    AND json_extract_scalar(CAST(event_properties AS json), '$.planType') IN ('Infographics', 'Regular subscription')
),

users_with_cancel AS (
  SELECT DISTINCT user_id
  FROM cancel_events
),

cancel_stats_any AS (
  SELECT
    user_id,
    COUNT(*) AS cancel_events_count_any,
    MIN(cancelation_completed_at) AS first_cancelation_completed_at_any,
    MAX(cancelation_completed_at) AS last_cancelation_completed_at_any
  FROM cancel_events
  GROUP BY 1
),

cancel_events_typed AS (
  SELECT
    user_id,
    cancelation_completed_at,
    plan_type,
    CASE
      WHEN plan_type = 'Infographics' THEN TRUE
      WHEN plan_type = 'Regular subscription' THEN FALSE
    END AS canceled_is_upsell
  FROM cancel_events
),

purchase_sessions AS (
  SELECT
    ps.user_id,
    ps.user_email,
    ps.purchase_completed_at,
    ps.subscription_created_at,
    ps.subscription_updated_at,
    ps.subscription_canceled_at,
    ps.vendor_subscription_id,
    ps.parent_subscription_id,
    ps.is_upsell,
    ps.product_id,
    cs.status AS subscription_status
  FROM data_silver_product_sessions_prod.sf_purchase_sessions ps
  LEFT JOIN chargebee_product_catalog_2.subscription cs
    ON ps.vendor_subscription_id = cs.id
  WHERE ps.user_id IN (SELECT user_id FROM users_with_cancel)
    AND ps.subscription_created_at >= (SELECT start_date FROM params)
),

sub_instances AS (
  SELECT
    user_id,
    vendor_subscription_id,
    parent_subscription_id,
    is_upsell,
    MIN(subscription_created_at) AS subscription_created_at,
    MAX(subscription_updated_at) AS subscription_updated_at,
    MAX(subscription_canceled_at) AS subscription_canceled_at,
    MAX(subscription_status) AS subscription_status,
    MAX(product_id) AS product_id
  FROM purchase_sessions
  GROUP BY 1,2,3,4
),

cancel_event_to_sub AS (
  SELECT
    ce.user_id,
    ce.cancelation_completed_at,
    ce.plan_type,
    ce.canceled_is_upsell,
    si.vendor_subscription_id AS canceled_subscription_id
  FROM cancel_events_typed ce
  JOIN sub_instances si
    ON si.user_id = ce.user_id
   AND si.is_upsell = ce.canceled_is_upsell
   AND si.subscription_canceled_at IS NOT NULL
   AND CAST(si.subscription_canceled_at AS DATE) = CAST(ce.cancelation_completed_at AS DATE)
),

cancel_per_subscription AS (
  SELECT
    user_id,
    canceled_subscription_id,
    MIN(cancelation_completed_at) AS first_cancelation_completed_at,
    MAX(cancelation_completed_at) AS last_cancelation_completed_at,
    COUNT(*) AS cancel_events_count
  FROM cancel_event_to_sub
  GROUP BY 1,2
),

/* ========= кандидаты "rebill after cancel" ========= */
/* Важно: rebill_purchase_completed_at здесь больше не выбираем.
   Используем purchase_sessions только как фильтр "после cancel был purchase". */

cand_upsell_cancel_rebill AS (
  SELECT
    cps.user_id,
    ps.user_email,
    cps.canceled_subscription_id AS cancel_trigger_subscription_id,
    si.vendor_subscription_id AS subscription_id,
    'upsell' AS subscription_type,
    cps.first_cancelation_completed_at,
    cps.cancel_events_count,
    si.subscription_created_at,
    si.subscription_status,
    si.product_id
  FROM cancel_per_subscription cps
  JOIN sub_instances si
    ON si.user_id = cps.user_id
   AND si.vendor_subscription_id = cps.canceled_subscription_id
   AND si.is_upsell = TRUE
  JOIN purchase_sessions ps
    ON ps.user_id = cps.user_id
   AND ps.vendor_subscription_id = cps.canceled_subscription_id
   AND ps.is_upsell = TRUE
   AND ps.purchase_completed_at > cps.first_cancelation_completed_at
  GROUP BY 1,2,3,4,5,6,7,8,9,10
),

cand_main_cancel_rebill_main AS (
  SELECT
    cps.user_id,
    ps.user_email,
    cps.canceled_subscription_id AS cancel_trigger_subscription_id,
    si.vendor_subscription_id AS subscription_id,
    'main' AS subscription_type,
    cps.first_cancelation_completed_at,
    cps.cancel_events_count,
    si.subscription_created_at,
    si.subscription_status,
    si.product_id
  FROM cancel_per_subscription cps
  JOIN sub_instances si
    ON si.user_id = cps.user_id
   AND si.vendor_subscription_id = cps.canceled_subscription_id
   AND si.is_upsell = FALSE
  JOIN purchase_sessions ps
    ON ps.user_id = cps.user_id
   AND ps.vendor_subscription_id = cps.canceled_subscription_id
   AND ps.is_upsell = FALSE
   AND ps.purchase_completed_at > cps.first_cancelation_completed_at
  GROUP BY 1,2,3,4,5,6,7,8,9,10
),

cand_main_cancel_rebill_upsell AS (
  SELECT
    cps.user_id,
    ps.user_email,
    main.vendor_subscription_id AS cancel_trigger_subscription_id,
    ups.vendor_subscription_id AS subscription_id,
    'upsell' AS subscription_type,
    cps.first_cancelation_completed_at,
    cps.cancel_events_count,
    ups.subscription_created_at,
    ups.subscription_status,
    ups.product_id
  FROM cancel_per_subscription cps
  JOIN sub_instances main
    ON main.user_id = cps.user_id
   AND main.vendor_subscription_id = cps.canceled_subscription_id
   AND main.is_upsell = FALSE
  JOIN sub_instances ups
    ON ups.user_id = cps.user_id
   AND ups.is_upsell = TRUE
   AND ups.parent_subscription_id = main.vendor_subscription_id
  JOIN purchase_sessions ps
    ON ps.user_id = ups.user_id
   AND ps.vendor_subscription_id = ups.vendor_subscription_id
   AND ps.is_upsell = TRUE
   AND ps.purchase_completed_at > cps.first_cancelation_completed_at
  GROUP BY 1,2,3,4,5,6,7,8,9,10
),

all_candidates AS (
  SELECT * FROM cand_upsell_cancel_rebill
  UNION ALL
  SELECT * FROM cand_main_cancel_rebill_main
  UNION ALL
  SELECT * FROM cand_main_cancel_rebill_upsell
),

dedup_per_subscription AS (
  SELECT *
  FROM (
    SELECT
      ac.*,
      ROW_NUMBER() OVER (
        PARTITION BY ac.user_id, ac.subscription_id
        ORDER BY ac.first_cancelation_completed_at ASC
      ) AS rn
    FROM all_candidates ac
  )
  WHERE rn = 1
)

SELECT
  d.user_id,
  d.user_email,
  d.subscription_id,
  d.subscription_type,
  d.cancel_trigger_subscription_id,
  d.first_cancelation_completed_at AS first_cancelation_event_at,
  d.subscription_created_at,
  tx.rebill_purchase_completed_at,
  tx.payment_id,
  d.subscription_status,
  d.product_id
FROM dedup_per_subscription d
LEFT JOIN latest_transaction_per_subscription tx
  ON d.subscription_id = tx.subscription_id
LEFT JOIN cancel_stats_any csany
  ON csany.user_id = d.user_id
ORDER BY d.first_cancelation_completed_at DESC, d.user_id, d.subscription_type, d.subscription_id;
 WITH trial_products(product_type, product_id, product_name_combined, plan) AS (
    SELECT
      *
    FROM (VALUES
      (
        'trial',
        'ms_ot_lifetime_5.19_USD',
        'ms_ts_1w_5.19_1m_30.95_USD',
        '1-week trial → 1-month'
      ),
      (
        'full',
        'ms_s_1week_5.19_1month_30.95_USD',
        'ms_ts_1w_5.19_1m_30.95_USD',
        '1-week trial → 1-month'
      ),
      (
        'trial',
        'ms_ot_lifetime_5.99_USD',
        'ms_ts_1w_5.99_1m_30.95_USD',
        '1-week trial → 1-month'
      ),
      (
        'full',
        'ms_s_1week_5.99_1month_30.95_USD',
        'ms_ts_1w_5.99_1m_30.95_USD',
        '1-week trial → 1-month'
      ),
      (
        'trial',
        'ms_ot_lifetime_6.93_24.98_USD',
        'ms_ts_1w_6.93_1m_24.98_USD',
        '1-week trial → 1-month'
      ),
      (
        'full',
        'ms_s_1week_6.93_1month_24.98_USD',
        'ms_ts_1w_6.93_1m_24.98_USD',
        '1-week trial → 1-month'
      ),
      (
        'trial',
        'ms_ot_lifetime_6.93_USD',
        'ms_ts_1w_6.93_1m_30.95_USD',
        '1-week trial → 1-month'
      ),
      (
        'full',
        'ms_s_1week_6.93_1month_30.95_USD',
        'ms_ts_1w_6.93_1m_30.95_USD',
        '1-week trial → 1-month'
      ),
      (
        'trial',
        'pro_01j5wm8mdhx7exw3hcdwkxs02d',
        'ms_ts_1w_6.93_1m_30.95_USD',
        '1-week trial → 1-month'
      ),
      (
        'full',
        'pro_01j5wmemxmqnzeykx4tptsem9y',
        'ms_ts_1w_6.93_1m_30.95_USD',
        '1-week trial → 1-month'
      ),
      (
        'trial',
        'pro_01jaddddgs26bh5pp8zy4x5z8r',
        'sm_ts_1w_6.93_1m_24.98_USD',
        '1-week trial → 1-month'
      ),
      (
        'full',
        'pro_01jaddbd1a5jrevrmqg3225eb8',
        'sm_ts_1w_6.93_1m_24.98_USD',
        '1-week trial → 1-month'
      ),
      (
        'trial',
        'pro_01jd6h7hhw0k671913563nkpsp',
        'nl_ts_1w_6.93_1m_30.95_USD',
        '1-week trial → 1-month'
      ),
      (
        'full',
        'pro_01jd6hztqk7nt2ey22ww29wwca',
        'nl_ts_1w_6.93_1m_30.95_USD',
        '1-week trial → 1-month'
      ),
      (
        'trial',
        'pro_01jk5xkmtw1rxghffn9d4cjn45',
        'sm_ts_1w_6.93_1m_20.39_USD',
        '1-week trial → 1-month'
      ),
      (
        'full',
        'pro_01jk5xn2epftffzpwvj1m3vyy5',
        'sm_ts_1w_6.93_1m_20.39_USD',
        '1-week trial → 1-month'
      ),
      (
        'trial',
        'pro_01jkb0td80wwaa9zxgqsj3823v',
        'ms_ts_1w_6.93_1m_30.95_USD',
        '1-week trial → 1-month'
      ),
      (
        'full',
        'pro_01jkb0rvb358abnx9tkaa4nqkk',
        'ms_ts_1w_6.93_1m_30.95_USD',
        '1-week trial → 1-month'
      ),
      (
        'trial',
        'prod_Qa5xQfrW9zYNzp',
        'ms_ts_1w_6.93_1m_30.95_USD',
        '1-week trial → 1-month'
      ),
      (
        'full',
        'prod_Qa5zVqDfEWKldZ',
        'ms_ts_1w_6.93_1m_30.95_USD',
        '1-week trial → 1-month'
      ),
      (
        'trial',
        'prod_R30OMubRpDP4Fr',
        'sm_ts_1w_6.93_1m_24.98_USD',
        '1-week trial → 1-month'
      ),
      (
        'full',
        'prod_R30OqnN2QDfLna',
        'sm_ts_1w_6.93_1m_24.98_USD',
        '1-week trial → 1-month'
      ),
      (
        'trial',
        'prod_RFyABQTAkOMXwE',
        'nl_ts_1w_6.93_1m_30.95_USD',
        '1-week trial → 1-month'
      ),
      (
        'full',
        'prod_RFyBVsjdJFlEZY',
        'nl_ts_1w_6.93_1m_30.95_USD',
        '1-week trial → 1-month'
      ),
      (
        'trial',
        'prod_RhoWgpb22JmWT3',
        'sm_ts_1w_6.93_1m_20.39_USD',
        '1-week trial → 1-month'
      ),
      (
        'full',
        'prod_RhoW7TyyL4T9gq',
        'sm_ts_1w_6.93_1m_20.39_USD',
        '1-week trial → 1-month'
      ),
      (
        'trial',
        'prod_RiYTWQ2SPtbntO',
        'ms_ts_1w_6.93_1m_30.95_USD',
        '1-week trial → 1-month'
      ),
      (
        'full',
        'prod_RiYTpzuJYzPY2h',
        'ms_ts_1w_6.93_1m_30.95_USD',
        '1-week trial → 1-month'
      ),
      (
        'trial',
        'prod_RQa4L9pyjabWvG',
        'sm_ts_1w_6.93_1m_24.98_USD',
        '1-week trial → 1-month'
      ),
      (
        'full',
        'prod_RQaCjwOoV3TX4V',
        'sm_ts_1w_6.93_1m_24.98_USD',
        '1-week trial → 1-month'
      ),
      (
        'trial',
        'prod_RQauRnvRdccyN2',
        'nl_ts_1w_6.93_1m_30.95_USD',
        '1-week trial → 1-month'
      ),
      (
        'full',
        'prod_RQavMko3rghXTd',
        'nl_ts_1w_6.93_1m_30.95_USD',
        '1-week trial → 1-month'
      ),
      (
        'trial',
        'prod_RQZByxvGdx7zwV',
        'ms_ts_1w_6.93_1m_30.95_USD',
        '1-week trial → 1-month'
      ),
      (
        'full',
        'prod_RQZCJQQdgeHlSf',
        'ms_ts_1w_6.93_1m_30.95_USD',
        '1-week trial → 1-month'
      ),
      (
        'trial',
        'sm_ot_lifetime_4.99_USD',
        'sm_ts_1w_4.99_1m_20.39_USD',
        '1-week trial → 1-month'
      ),
      (
        'full',
        'sm_s_1week_4.99_1month_20.39_USD',
        'sm_ts_1w_4.99_1m_20.39_USD',
        '1-week trial → 1-month'
      ),
      (
        'trial',
        'sm_ot_lifetime_5.99_USD',
        'sm_ts_1w_5.99_1m_20.39_USD',
        '1-week trial → 1-month'
      ),
      (
        'full',
        'sm_s_1week_5.99_1month_20.39_USD',
        'sm_ts_1w_5.99_1m_20.39_USD',
        '1-week trial → 1-month'
      ),
      (
        'trial',
        'sm_ot_lifetime_6.93_24.98_USD',
        'sm_ts_1w_6.93_1m_24.98_USD',
        '1-week trial → 1-month'
      ),
      (
        'full',
        'sm_s_1week_6.93_1month_24.98_USD',
        'sm_ts_1w_6.93_1m_24.98_USD',
        '1-week trial → 1-month'
      ),
      (
        'trial',
        'sm_ot_lifetime_6.93_USD',
        'sm_ts_1w_6.93_1m_20.39_USD',
        '1-week trial → 1-month'
      ),
      (
        'full',
        'sm_s_1week_6.93_1month_20.39_USD',
        'sm_ts_1w_6.93_1m_20.39_USD',
        '1-week trial → 1-month'
      ),
      (
        'trial',
        'at_ot_lifetime_6.93_24.98_USD',
        'at_ts_1w_6.93_1m_24.98_USD',
        '1-week trial → 1-month'
      ),
      (
        'full',
        'at_s_1week_6.93_1month_24.98_USD',
        'at_ts_1w_6.93_1m_24.98_USD',
        '1-week trial → 1-month'
      )) AS _values
  ),

     seeds AS (
    SELECT
      t1.*,
      trial_products.product_type AS trial_product_type,
      trial_products.product_name_combined,
      trial_products.plan
    FROM (
      SELECT
        vendor_product_id,
        vendor_product_name,
        is_upsell,
        product_type,
        ROW_NUMBER() OVER (PARTITION BY vendor_product_id ORDER BY _dlt_load_id DESC) AS rn
      FROM bronze_postgres_ltv.automated_product_items
    ) AS t1
    LEFT JOIN trial_products
      ON t1.vendor_product_id = trial_products.product_id
    WHERE
      t1.rn = 1
  ),
     sub_status AS (
    SELECT
      subscription_id,
      CASE
        WHEN sub_status IN ('cancelled', 'canceled', 'non_renewing')
        THEN 'cancelled'
        ELSE sub_status
      END AS subscription_status
    FROM prod_silver_chargebee.chargebee_subscription
  ),
     ff_upsell AS (
    SELECT
      user_id,
      LOWER(user_email) AS email,
      project_name,
      web_funnel_short,
      vendor,
      product_id,
      price_id,
      discount_id,
      paid_amount_usd,
      is_upsell,
      vendor_subscription_id,
      vendor_transaction_id,
      purchase_completed_at,
      charge_refunded_at,
      subscription_created_at,
      current_period_end
    FROM prod_silver_product_sessions.user_purchase_transactions_sessions
    WHERE
      vendor = 'chargebee'
      AND is_upsell
      AND purchase_completed > 0
      AND NOT product_id IS NULL
      AND NOT vendor_subscription_id IS NULL
      AND subscription_created_at >= CAST('2025-01-01' AS DATE)
  ),
     ff_main AS (
    SELECT
      user_id,
      LOWER(user_email) AS email,
      project_name,
      web_funnel_short,
      vendor,
      product_id,
      price_id,
      discount_id,
      paid_amount_usd,
      is_upsell,
      vendor_subscription_id,
      vendor_transaction_id,
      purchase_completed_at,
      charge_refunded_at,
      subscription_created_at,
      current_period_end
    FROM prod_silver_product_sessions.user_purchase_transactions_sessions
    WHERE
      vendor = 'chargebee'
      AND NOT is_upsell
      AND purchase_completed > 0
      AND NOT product_id IS NULL
      AND NOT vendor_subscription_id IS NULL
      AND subscription_created_at >= CAST('2025-01-01' AS DATE)
  ),
     sf AS (
    SELECT
      user_id,
      LOWER(user_email) AS email,
      project_name,
      web_funnel_short,
      vendor,
      product_id,
      price_id,
      discount_id,
      paid_amount_usd,
      is_upsell,
      vendor_subscription_id,
      vendor_transaction_id,
      purchase_completed_at,
      charge_refunded_at,
      subscription_created_at,
      current_period_end
    FROM prod_silver_product_sessions.sf_purchase_transactions_sessions
    WHERE
      vendor = 'chargebee'
      AND purchase_completed > 0
      AND NOT product_id IS NULL
      AND NOT vendor_subscription_id IS NULL
      AND subscription_created_at >= CAST('2025-01-01' AS DATE)
  ), all_chargebee_transactions AS (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY vendor_transaction_id ORDER BY purchase_completed_at DESC) AS rn
    FROM (
      SELECT
        *
      FROM ff_main
      UNION ALL
      SELECT
        *
      FROM ff_upsell
      UNION ALL
      SELECT
        *
      FROM sf
    )
  ), full_union AS (
    SELECT
      sub.user_id,
      sub.email,
      sub.vendor,
      sub.product_id,
      COALESCE(seeds.is_upsell, sub.is_upsell) AS is_upsell,
      sub.vendor_subscription_id AS subscription_id,
      MIN(sub.subscription_created_at) OVER (PARTITION BY sub.vendor_subscription_id) AS subscription_created_at,
      MAX(sub.current_period_end) OVER (PARTITION BY sub.vendor_subscription_id) AS subscription_ends_at,
      sub.purchase_completed_at,
      sub_status.subscription_status
    FROM all_chargebee_transactions AS sub
    LEFT JOIN sub_status
      ON sub.vendor_subscription_id = sub_status.subscription_id
    LEFT JOIN seeds
      ON sub.product_id = seeds.vendor_product_id
    WHERE
      sub.rn = 1
      AND NOT REGEXP_LIKE(sub.email, 'test|ppkostenko|stellarlab|xironikys|pikachy94|lampalampa|shelest')
  ), chargebee_subscriptions AS (
    SELECT DISTINCT
      user_id,
      email,
      subscription_id,
      is_upsell,
      subscription_status,
      subscription_created_at
    FROM full_union
  ), ranked_subscriptions /* **NEW**: Ranking subscriptions for each user to find the latest ones */ AS (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY user_id, is_upsell ORDER BY subscription_created_at DESC) AS sub_rank
    FROM chargebee_subscriptions
  ), cancelled_main_subscriptions /* Finding the LATEST main subscription that is cancelled */ AS (
    SELECT
      user_id,
      email,
      subscription_id,
      subscription_status,
      subscription_created_at
    FROM ranked_subscriptions
    WHERE
      is_upsell = FALSE
      AND subscription_status = 'cancelled'
      AND sub_rank = 1 /* Filter for the latest one */
  ), active_upsell_subscriptions /* Finding the LATEST upsell subscription that is active */ AS (
    SELECT
      user_id,
      email,
      subscription_id,
      subscription_status,
      subscription_created_at
    FROM ranked_subscriptions
    WHERE
      is_upsell = TRUE
      AND subscription_status = 'active'
      AND sub_rank = 1 /* Filter for the latest one */
  )
  /* Final join remains the same, but now operates on the latest subscriptions */
  SELECT
    main.user_id,
    main.email,
    main.subscription_id AS main_subscription_id,
    main.subscription_status AS main_subscription_status,
    main.subscription_created_at AS main_subscription_creation_date,
    upsell.subscription_id AS upsell_subscription_id,
    upsell.subscription_status AS upsell_subscription_status,
    upsell.subscription_created_at AS upsell_subscription_creation_date
  FROM cancelled_main_subscriptions AS main
  JOIN active_upsell_subscriptions AS upsell
    ON main.email = upsell.email
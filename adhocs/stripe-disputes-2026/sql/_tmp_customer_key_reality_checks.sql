WITH customer_dim AS (
  SELECT DISTINCT psp_account_name, id AS customer_id
  FROM data_bronze_stripe_prod.stripe_customer
  WHERE id IS NOT NULL
),
charge_succeeded_chk AS (
  SELECT
    'stripe_charge_succeeded.data__object__customer' AS source,
    COUNT(*) AS rows_with_key,
    COUNT_IF(c.customer_id IS NOT NULL) AS matched_rows
  FROM data_bronze_stripe_prod.stripe_charge_succeeded t
  LEFT JOIN customer_dim c
    ON t.psp_account_name = c.psp_account_name
   AND t.data__object__customer = c.customer_id
  WHERE t.data__object__customer IS NOT NULL
),
charge_captured_chk AS (
  SELECT
    'stripe_charge_captured.data__object__customer' AS source,
    COUNT(*) AS rows_with_key,
    COUNT_IF(c.customer_id IS NOT NULL) AS matched_rows
  FROM data_bronze_stripe_prod.stripe_charge_captured t
  LEFT JOIN customer_dim c
    ON t.psp_account_name = c.psp_account_name
   AND t.data__object__customer = c.customer_id
  WHERE t.data__object__customer IS NOT NULL
),
charge_failed_chk AS (
  SELECT
    'stripe_charge_failed.data__object__customer' AS source,
    COUNT(*) AS rows_with_key,
    COUNT_IF(c.customer_id IS NOT NULL) AS matched_rows
  FROM data_bronze_stripe_prod.stripe_charge_failed t
  LEFT JOIN customer_dim c
    ON t.psp_account_name = c.psp_account_name
   AND t.data__object__customer = c.customer_id
  WHERE t.data__object__customer IS NOT NULL
),
payment_intent_created_chk AS (
  SELECT
    'stripe_payment_intent_created.data__object__customer' AS source,
    COUNT(*) AS rows_with_key,
    COUNT_IF(c.customer_id IS NOT NULL) AS matched_rows
  FROM data_bronze_stripe_prod.stripe_payment_intent_created t
  LEFT JOIN customer_dim c
    ON t.psp_account_name = c.psp_account_name
   AND t.data__object__customer = c.customer_id
  WHERE t.data__object__customer IS NOT NULL
),
payment_intent_succeeded_chk AS (
  SELECT
    'stripe_payment_intent_succeeded.data__object__customer' AS source,
    COUNT(*) AS rows_with_key,
    COUNT_IF(c.customer_id IS NOT NULL) AS matched_rows
  FROM data_bronze_stripe_prod.stripe_payment_intent_succeeded t
  LEFT JOIN customer_dim c
    ON t.psp_account_name = c.psp_account_name
   AND t.data__object__customer = c.customer_id
  WHERE t.data__object__customer IS NOT NULL
),
payment_method_attached_chk AS (
  SELECT
    'stripe_payment_method_attached.data__object__customer' AS source,
    COUNT(*) AS rows_with_key,
    COUNT_IF(c.customer_id IS NOT NULL) AS matched_rows
  FROM data_bronze_stripe_prod.stripe_payment_method_attached t
  LEFT JOIN customer_dim c
    ON t.psp_account_name = c.psp_account_name
   AND t.data__object__customer = c.customer_id
  WHERE t.data__object__customer IS NOT NULL
)
SELECT
  source,
  rows_with_key,
  matched_rows,
  CAST(100.0 * matched_rows / NULLIF(rows_with_key, 0) AS DECIMAL(5,2)) AS match_pct
FROM (
  SELECT * FROM charge_succeeded_chk
  UNION ALL SELECT * FROM charge_captured_chk
  UNION ALL SELECT * FROM charge_failed_chk
  UNION ALL SELECT * FROM payment_intent_created_chk
  UNION ALL SELECT * FROM payment_intent_succeeded_chk
  UNION ALL SELECT * FROM payment_method_attached_chk
)
ORDER BY source;

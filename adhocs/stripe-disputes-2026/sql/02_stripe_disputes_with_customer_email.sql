WITH charge_latest AS (
  SELECT
    psp_account_name,
    data__object__id AS charge_id,
    data__object__customer AS customer_id,
    row_number() OVER (
      PARTITION BY psp_account_name, data__object__id
      ORDER BY created DESC, _ingested_at DESC
    ) AS rn
  FROM data_bronze_stripe_prod.stripe_charge_succeeded
  WHERE data__object__customer IS NOT NULL
),
customer_latest AS (
  SELECT
    psp_account_name,
    id AS customer_id,
    lower(email) AS customer_email,
    row_number() OVER (
      PARTITION BY psp_account_name, id
      ORDER BY created DESC, _ingested_at DESC
    ) AS rn
  FROM data_bronze_stripe_prod.stripe_customer
  WHERE email IS NOT NULL
)
SELECT
  'stripe' AS provider,
  'stellartech' AS company_account,
  dc.psp_account_name AS merchant_account,
  CAST(from_unixtime(dc.created) AS DATE) AS date,
  from_unixtime(dc.created) AS opened_at,
  NULL AS record_date,
  dc.data__object__id AS dispute_id,
  dc.data__object__currency AS currency,
  dc.data__object__amount AS amount_raw,
  dc.data__object__amount / 100.0 AS amount,
  dc.data__object__reason AS reason,
  dc.data__object__payment_method_details__card__case_type AS case_type,
  dc.data__object__status AS status,
  dc.data__object__evidence_details__has_evidence AS has_evidence,
  dc.data__object__evidence_details__submission_count AS submission_count,
  cu.customer_email AS user_email
FROM data_bronze_stripe_prod.stripe_charge_dispute_created dc
LEFT JOIN charge_latest ch
  ON dc.psp_account_name = ch.psp_account_name
 AND dc.data__object__charge = ch.charge_id
 AND ch.rn = 1
LEFT JOIN customer_latest cu
  ON ch.psp_account_name = cu.psp_account_name
 AND ch.customer_id = cu.customer_id
 AND cu.rn = 1;

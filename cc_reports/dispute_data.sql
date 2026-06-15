WITH
      adyen_raw AS (
SELECT
    CAST(dispute_date AS TIMESTAMP) AS created_at,
    '01_created' AS record_type,
    'adyen' AS provider,
    company_account,
    merchant_account,
    CAST(date_parse(dispute_date, '%Y-%m-%d %H:%i:%s') AS DATE)  AS date,
    CAST(dispute_date AS TIMESTAMP) AS updated_at,
    record_date,
    dispute_psp_reference AS dispute_id,
    dispute_currency AS currency,
    dispute_amount as amount_raw,
    dispute_amount AS amount,
    dispute_reason AS reason,
    record_type AS case_type,
    status,
    null as life_cycle_stage,
    null as channel,
    null as outcome_code,
    null as issue_type,
    null as has_evidence,
    null as submission_count,
    ROW_NUMBER() OVER(PARTITION BY dispute_psp_reference ORDER BY record_date DESC) AS rn
FROM adyen_prod.dispute_report
WHERE 1=1
  AND CAST(date_parse(dispute_date, '%Y-%m-%d %H:%i:%s') AS DATE) >= DATE '2025-10-01'
),
    paypal_data AS (
SELECT
    CAST(create_time AS TIMESTAMP) AS created_at,
    '01_created' AS record_type,
    'PayPal' AS provider,
    'Stellartech' AS company_account,
    'Apextech' AS merchant_account,
    CAST(create_time AS DATE) AS date,
    CAST(create_time AS TIMESTAMP) AS updated_at,
    NULL AS record_date,
    id AS dispute_id,
    currency_code AS currency,
    amount as amount_raw,
    amount,
    reason,
    null AS case_type,
    status,
    life_cycle_stage,
    channel,
    outcome_code,
    issue_type,
    null as has_evidence,
    null as submission_count
FROM fivetran_paypal_prod_apextech.dispute
WHERE 1=1
  AND CAST(create_time AS DATE) >= CAST('2025-10-01' AS DATE)
UNION ALL
SELECT
    CAST(create_time AS TIMESTAMP) AS created_at,
    '01_created' AS record_type,
    'PayPal' AS provider,
    'Stellartech' AS company_account,
    'Nexera' AS merchant_account,
    CAST(create_time AS DATE) AS date,
    CAST(create_time AS TIMESTAMP) AS updated_at,
    NULL AS record_date,
    id AS dispute_id,
    currency_code AS currency,
    amount as amount_raw,
    amount,
    reason,
    null AS case_type,
    status,
    life_cycle_stage,
    channel,
    outcome_code,
    issue_type,
    null as has_evidence,
    null as submission_count
FROM fivetran_paypal_prod_nexera.dispute
WHERE 1=1
  AND CAST(create_time AS DATE) >= CAST('2025-10-01' AS DATE)
UNION ALL
SELECT
    CAST(create_time AS TIMESTAMP) AS created_at,
    '01_created' AS record_type,
    'PayPal' AS provider,
    'Stellartech' AS company_account,
    'Stellartech LTD' AS merchant_account,
    CAST(create_time AS DATE) AS date,
    CAST(create_time AS TIMESTAMP) AS updated_at,
    NULL AS record_date,
    id AS dispute_id,
    currency_code AS currency,
    amount as amount_raw,
    amount,
    reason,
    null AS case_type,
    status,
    life_cycle_stage,
    channel,
    outcome_code,
    issue_type,
    null as has_evidence,
    null as submission_count
FROM fivetran_paypal_prod.dispute
WHERE 1=1
  AND CAST(create_time AS DATE) >= CAST('2025-10-01' AS DATE)
),
        stripe_created AS (
SELECT '01_created' as record_type,
       'stripe' as provider,
       'stellartech' as company_account,
       psp_account_name as merchant_account,
       CAST(from_unixtime(created) as DATE) as date,
       CAST(from_unixtime(created) AS TIMESTAMP) as updated_at,
       null as record_date,
       data__object__id as dispute_id,
       data__object__currency as currency,
       data__object__amount as amount_raw,
       data__object__amount/100.0 as amount,
       data__object__reason as reason,
       data__object__payment_method_details__card__case_type as case_type,
       data__object__status as status,
       null as life_cycle_stage,
       null as channel,
       null as outcome_code,
       null as issue_type,
       data__object__evidence_details__has_evidence as has_evidence,
       data__object__evidence_details__submission_count as submission_count,
       ROW_NUMBER() OVER (PARTITION BY data__object__id ORDER BY from_unixtime(created) DESC) as rn
FROM data_bronze_stripe_prod.stripe_charge_dispute_created
WHERE 1=1
  AND CAST(from_unixtime(created) as DATE) >= CAST('2025-10-01' AS DATE)
),
    stripe_updated AS (
SELECT '02_updated' as record_type,
       'stripe' as provider,
       'stellartech' as company_account,
       psp_account_name as merchant_account,
       CAST(from_unixtime(created) as DATE) as date,
       CAST(from_unixtime(created) as TIMESTAMP) as updated_at,
       null as record_date,
       data__object__id as dispute_id,
       data__object__currency as currency,
       data__object__amount as amount_raw,
       data__object__amount/100.0 as amount,
       data__object__reason as reason,
       data__object__payment_method_details__card__case_type as case_type,
       data__object__status as status,
       null as life_cycle_stage,
       null as channel,
       null as outcome_code,
       null as issue_type,
       data__object__evidence_details__has_evidence as has_evidence,
       data__object__evidence_details__submission_count as submission_count,
       ROW_NUMBER() OVER (PARTITION BY data__object__id ORDER BY from_unixtime(created) DESC) as rn
FROM data_bronze_stripe_prod.stripe_charge_dispute_updated
WHERE 1=1
  AND CAST(from_unixtime(created) as DATE) >= CAST('2025-10-01' AS DATE)
),
    stripe_closed AS (
SELECT '03_closed' as record_type,
       'stripe' as provider,
       'stellartech' as company_account,
       psp_account_name as merchant_account,
       CAST(from_unixtime(created) as DATE) as date,
       CAST(from_unixtime(created) as TIMESTAMP) as updated_at,
       null as record_date,
       data__object__id as dispute_id,
       data__object__currency as currency,
       data__object__amount as amount_raw,
       data__object__amount/100.0 as amount,
       data__object__reason as reason,
       data__object__payment_method_details__card__case_type as case_type,
       data__object__status as status,
       null as life_cycle_stage,
       null as channel,
       null as outcome_code,
       null as issue_type,
       data__object__evidence_details__has_evidence as has_evidence,
       data__object__evidence_details__submission_count as submission_count,
       ROW_NUMBER() OVER (PARTITION BY data__object__id ORDER BY from_unixtime(created) DESC) as rn
FROM data_bronze_stripe_prod.stripe_charge_dispute_closed
WHERE 1=1
  AND CAST(from_unixtime(created) as DATE) >= CAST('2025-10-01' AS DATE)
),
    stripe_combined AS (
SELECT *,
       ROW_NUMBER() OVER (PARTITION BY dispute_id ORDER BY record_type DESC, rn DESC) as actuality_flag
FROM (
SELECT * FROM stripe_closed
UNION ALL
SELECT * FROM stripe_updated
UNION ALL
SELECT * FROM stripe_created
) q
),
    stripe_prepared AS (
SELECT CAST(from_unixtime(dc.created) AS TIMESTAMP) as created_at,
       sc.*
FROM stripe_combined sc
    LEFT JOIN data_bronze_stripe_prod.stripe_charge_dispute_created dc ON sc.dispute_id = dc.data__object__id
WHERE 1=1
  AND actuality_flag = 1
),
    united_dispite_data AS (
SELECT
    created_at,
    record_type,
    provider,
    company_account,
    merchant_account,
    date,
    updated_at,
    record_date,
    dispute_id,
    currency,
    amount_raw,
    amount,
    reason,
    case_type,
    status,
    life_cycle_stage,
    channel,
    outcome_code,
    issue_type,
    has_evidence,
    submission_count
FROM adyen_raw
WHERE 1=1
  AND rn = 1
UNION ALL
SELECT
    created_at,
    record_type,
    provider,
    company_account,
    merchant_account,
    date,
    updated_at,
    record_date,
    dispute_id,
    currency,
    amount_raw,
    amount,
    reason,
    case_type,
    status,
    life_cycle_stage,
    channel,
    outcome_code,
    issue_type,
    has_evidence,
    submission_count
FROM paypal_data
UNION ALL
SELECT
    created_at,
    record_type,
    provider,
    company_account,
    merchant_account,
    date,
    updated_at,
    record_date,
    dispute_id,
    currency,
    amount_raw,
    amount,
    reason,
    case_type,
    status,
    life_cycle_stage,
    channel,
    outcome_code,
    issue_type,
    has_evidence,
    submission_count
FROM stripe_prepared
)

SELECT *
FROM united_dispite_data

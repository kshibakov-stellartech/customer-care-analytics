WITH stripe_data AS (
SELECT 'stripe' as provider,
       'stellartech' as company_account,
       psp_account_name as merchant_account,
       CAST(from_unixtime(created) as DATE) as date,
       from_unixtime(created) as opened_at,
       null as record_date,
       data__object__id as dispute_id,
       data__object__currency as currency,
       data__object__amount as amount_raw,
       data__object__amount/100.0 as amount,
       data__object__reason as reason,
       data__object__payment_method_details__card__case_type as case_type,
       data__object__status as status,
       data__object__evidence_details__has_evidence as has_evidence,
       data__object__evidence_details__submission_count as submission_count
FROM data_bronze_stripe_prod.stripe_charge_dispute_created
WHERE 1=1
  AND CAST(from_unixtime(created) as DATE) >= CAST('2025-10-01' AS DATE)
),
    adyen_raw AS (
SELECT
    'adyen' AS provider,
    company_account,
    merchant_account,
    CAST(date_parse(dispute_date, '%Y-%m-%d %H:%i:%s') AS DATE)  AS date,
    dispute_date AS opened_at,
    record_date,
    dispute_psp_reference AS dispute_id,
    dispute_currency AS currency,
    dispute_amount as amount_raw,
    dispute_amount AS amount,
    dispute_reason AS reason,
    record_type AS case_type,
    status,
    null as has_evidence,
    null as submission_count,
    ROW_NUMBER() OVER(PARTITION BY dispute_psp_reference ORDER BY record_date DESC) AS rn
FROM adyen_prod.dispute_report
WHERE 1=1
  --AND dispute_psp_reference = 'F82WCVJDR6N6KCG3'
),
    adyen_data AS (
SELECT provider,
    company_account,
    merchant_account,
    date,
    opened_at,
    record_date,
    dispute_id,
    currency,
    amount_raw,
    amount,
    reason,
    case_type,
    status,
    has_evidence,
    submission_count
FROM adyen_raw
WHERE 1=1
  AND rn = 1
  AND date >= DATE '2025-10-01'
)

/*_____________________________________________*/

SELECT 'stripe' as provider,
       'stellartech' as company_account,
       psp_account_name as merchant_account,
       CAST(from_unixtime(created) as DATE) as date,
       from_unixtime(created) as opened_at,
       null as record_date,
       data__object__id as dispute_id,
       data__object__currency as currency,
       data__object__amount as amount_raw,
       data__object__amount/100.0 as amount,
       data__object__reason as reason,
       data__object__payment_method_details__card__case_type as case_type,
       data__object__status as status,
       data__object__evidence_details__has_evidence as has_evidence,
       data__object__evidence_details__submission_count as submission_count
FROM data_bronze_stripe_prod.stripe_charge_dispute_created
WHERE 1=1
  AND CAST(from_unixtime(created) as DATE) >= CAST('2025-10-01' AS DATE)
;

SELECT
    'paypal' AS provider,
    'stellartech' AS company_account,
    'stellartech LTD' AS merchant_account,
    CAST(CONVERT_TIMEZONE('UTC', 'Europe/Paris', create_time) AS DATE) AS date,
    CONVERT_TIMEZONE('UTC', 'Europe/Paris', create_time) AS opened_at,
    NULL AS record_date,
    id AS dispute_id,
    currency_code AS currency,
    amount as amount_raw,
    amount,
    reason,
    'Chargeback' AS case_type,
    CASE
        WHEN outcome_code = 'RESOLVED_WITH_PAYOUT' THEN 'Win'
        ELSE status
    END AS status,
    null as has_evidence,
    null as submission_count
FROM fivetran_paypal_prod.dispute
--WHERE id = 'PP-R-LSC-580269454'
;

SELECT *
FROM fivetran_paypal_prod.dispute
WHERE id = 'PP-R-DPZ-614410637'
LIMIT 100
;

SELECT *
FROM fivetran_paypal_prod_apextech.dispute
WHERE 1=1
  --AND id = 'PP-R-UWC-614410432'
LIMIT 100
;

SELECT *
FROM (
SELECT *
FROM fivetran_paypal_prod.dispute
UNION ALL
SELECT *
FROM fivetran_paypal_prod_apextech.dispute
) q
WHERE 1=1
  --AND id = 'PP-R-UWC-614410432'
  AND outcome_code = 'RESOLVED_BUYER_FAVOUR'
  AND create_time >= DATE '2026-01-01'
  AND create_time <= DATE '2026-03-01'
LIMIT 200
;

WITH stripe_data AS (
SELECT 'stripe' as provider,
       'stellartech' as company_account,
       dc.psp_account_name as merchant_account,
       CAST(from_unixtime(dc.created) as DATE) as date,
       from_unixtime(dc.created) as opened_at,
       null as record_date,
       dc.data__object__id as dispute_id,
       dc.data__object__currency as currency,
       dc.data__object__amount as amount_raw,
       dc.data__object__amount/100.0 as amount,
       dc.data__object__reason as reason,
       dc.data__object__payment_method_details__card__case_type as case_type,
       COALESCE(du.data__object__status, dc.data__object__status) as status,
       du.data__object__evidence_details__has_evidence as has_evidence,
       du.data__object__evidence_details__submission_count as submission_count,
       ROW_NUMBER() OVER (PARTITION BY dc.data__object__id ORDER BY from_unixtime(du.created) DESC) as row_num
FROM data_bronze_stripe_prod.stripe_charge_dispute_created dc
    LEFT JOIN data_bronze_stripe_prod.stripe_charge_dispute_updated du ON dc.data__object__id = du.data__object__id
WHERE 1=1
  AND CAST(from_unixtime(dc.created) as DATE) >= CAST('2025-10-01' AS DATE)
)

SELECT date,
       status,
       submission_count,
       COUNT(1) as cnt
FROM stripe_data
WHERE 1=1
  AND row_num = 1
  AND status = 'lost'
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 2, 3
;

SELECT *
FROM data_bronze_stripe_prod.stripe_charge_dispute_updated
WHERE 1=1
  AND data__object__id = 'du_1SGm1fJ57MVrbnpuVOWRdCpv'
;

SELECT *
FROM firehose_typeform_1day_webhook_stage.typeform_1day
LIMIT 10;

SELECT *
  FROM firehose_typeform_1day_webhook_prod.typeform_1day
  WHERE _kinesis__ts >= current_timestamp - interval '1' day;

SELECT *
FROM firehose_typeform_3dayplus_webhook_stage.typeform_3dayplus
LIMIT 10;

SELECT project.*,
       task.*
FROM fivetran_asana.project
    LEFT JOIN fivetran_asana.project_task ON project.id = project_task.project_id
    LEFT JOIN fivetran_asana.task ON project_task.task_id = task.id
WHERE 1=1
  AND project.id = '1209882467788483'
LIMIT 100
;

SELECT  *
FROM data_silver_appfollow_prod.appfollow_reviews
WHERE 1=1
  AND author = 'Brandy Diekevers'
;

SELECT *
FROM data_bronze_appfollow_prod.appfollow_api
WHERE 1=1
  AND author = 'Brandy Diekevers'
;

SELECT *
FROM fivetran_paypal_prod.adjudication
WHERE 1=1
  AND dispute_id IN (
'PP-R-UWS-589828539'
)
;

WITh tbl AS (
SELECT id, channel, status, life_cycle_stage, outcome_refunded_amount, outcome_code, reason,
       ROW_NUMBER() OVER(PARTITION BY outcome_code ORDER BY id DESC) as rn
FROM fivetran_paypal_prod.dispute

WHERE 1=1
  AND life_cycle_stage = 'PRE_ARBITRATION'
)

SELECT *
FROM tbl
WHERE rn=1
;

SELECT id, channel, status, life_cycle_stage, outcome_refunded_amount, outcome_code, reason
FROM fivetran_paypal_prod.dispute
WHERE 1=1
  AND id IN (
'PP-R-UWS-589828539'
)
;

SELECT *
FROM data_bronze_stripe_prod.stripe_charge_dispute_created dc
LIMIT 100
;

SELECT *
FROM data_bronze_stripe_prod.stripe_charge_dispute_created dc
WHERE 1=1
  AND  CAST(from_unixtime(created) as DATE) BETWEEN DATE '2026-03-01' AND DATE '2026-04-01'
;

SELECT *
FROM adyen_prod.dispute_report
WHERE 1=1
  AND CAST(date_parse(dispute_date, '%Y-%m-%d %H:%i:%s') AS DATE) BETWEEN DATE '2026-03-01' AND DATE '2026-04-01'
;



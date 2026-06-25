SELECT
       'stripe' as provider,
       'stellartech' as company_account,
       dc.psp_account_name as merchant_account,
       CAST(from_unixtime(dc.created) as DATE) as date,
       from_unixtime(dc.created) as opened_at,
       null as record_date,
       dc.data__object__id as dispute_id,
       dc.data__object__currency as currency,
       dc.data__object__amount as amount_raw,
       dc.data__object__amount / 100.0 as amount,
       dc.data__object__reason as reason,
       dc.data__object__payment_method_details__card__case_type as case_type,
       dc.data__object__status as status,
       dc.data__object__evidence_details__has_evidence as has_evidence,
       dc.data__object__evidence_details__submission_count as submission_count,
       COALESCE(
         dc.data__object__evidence__customer_email_address,
         ch.data__object__billing_details__email,
         ch.data__object__metadata__customer_email
       ) as user_email
FROM data_bronze_stripe_prod.stripe_charge_dispute_created dc
LEFT JOIN data_bronze_stripe_prod.stripe_charge_succeeded ch
  ON dc.data__object__charge = ch.data__object__id
 AND dc.psp_account_name = ch.psp_account_name
LIMIT 100
;

SELECT *
FROM data_bronze_stripe_prod.stripe_customer
LIMIT 100
;

SELECT *
FROM data_bronze_zendesk_prod.zendesk_ticket_fields
;

WITH charge_latest AS (
  SELECT
    psp_account_name,
    data__object__id AS charge_id,
    data__object__customer AS customer_id,
    row_number() OVER (
      PARTITION BY psp_account_name, data__object__id
      ORDER BY created DESC, _ingested_at DESC
    ) AS rn
  FROM data_bronze_stripe_prod.вшч
  WHERE data__object__customer IS NOT NULL
),
customer_latest AS (
  SELECT
    psp_account_name,
    id AS customer_id,
    lower(email) AS user_email,
    row_number() OVER (
      PARTITION BY psp_account_name, id
      ORDER BY created DESC, _ingested_at DESC
    ) AS rn
  FROM data_bronze_stripe_prod.stripe_customer
  WHERE email IS NOT NULL
)
SELECT
  dc.*,
  cu.user_email
FROM data_bronze_stripe_prod.stripe_charge_dispute_created dc
LEFT JOIN charge_latest ch
  ON dc.psp_account_name = ch.psp_account_name
 AND dc.data__object__charge = ch.charge_id
 AND ch.rn = 1
LEFT JOIN customer_latest cu
  ON ch.psp_account_name = cu.psp_account_name
 AND ch.customer_id = cu.customer_id
 AND cu.rn = 1
;

SELECT dc.id,
       dc.created,
       dc.data__object__balance_transaction,
       dc.data__object__charge,
       dc.data__object__evidence__customer_email_address as mail_raw
FROM data_bronze_stripe_prod.stripe_charge_dispute_created dc
    LEFt JOIN
LIMIT 100
;

SELECT *
FROM data_bronze_stripe_prod.stripe_charge_dispute_created dc
LIMIT 10
;
/*
txn_1TPmBXJ57MVrbnpuj33luWhy	ch_3TJ0YDJ57MVrbnpu06MZbofg
txn_1TPhoDJ57MVrbnpupTyOeBxj	ch_3TP9oHJ57MVrbnpu1YseNAiZ
txn_1TPhkzJ57MVrbnpuFPTPMPHa	ch_3TOXQVJ57MVrbnpu1Tco7kMx
txn_1TPhccJ57MVrbnpuGqin7bpC	ch_3TLYM0J57MVrbnpu1TIGoaRg
txn_1TPhjhJ57MVrbnpuoiP475cR	ch_3TOXQKJ57MVrbnpu1Kqt9aGb
txn_1TPhgRJ57MVrbnpuSfscOaJ0	ch_3TLuhRJ57MVrbnpu09Y4vg1C
txn_1TPhYjJ57MVrbnpuEcP1iVTU	ch_3T0lNHJ57MVrbnpu1yBw6J28
txn_1TPhpNJ57MVrbnpuUOIdkfw2	ch_3TMI5hJ57MVrbnpu0s5KzlK7
txn_1TPhonJ57MVrbnpubPy81csF	ch_3TMI5YJ57MVrbnpu0xYz2LdR
txn_1TPhfwJ57MVrbnpuy9JdcFCQ	ch_3TKxUDJ57MVrbnpu1cQgWIFv
*/

SELECT *
FROM data_bronze_stripe_prod.stripe_charge_updated
WHERE 1=1
  AND data__object__id = 'ch_3TOXQVJ57MVrbnpu1Tco7kMx'
  /*AND data__object__metadata__customer_email IN (
'lhupje@yahoo.co.uk',
'marianazregan@gmail.com',
'litzy1aragon@yahoo.com',
'aspainter7@gmail.com',
'kddvt53@gmail.com',
'jweibel423@yahoo.com',
'jweibel423@yahoo.com',
'lidobeach@icloud.com',
'lidobeach@icloud.com',
'hcampos@windermereca.com',
'csalamancaq@gmail.com',
'jennifershay@myyahoo.com',
'nataliabilko3@gmail.com',
'thomsk1@verizon.net'
)*/

;

SELECT *
FROM data_bronze_stripe_prod.stripe_charge_dispute_created
LIMIT 100
;
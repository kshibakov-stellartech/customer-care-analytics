SELECT table_name, column_name
FROM information_schema.columns
WHERE table_schema='data_bronze_stripe_prod'
  AND table_name IN (
    'stripe_charge_dispute_created',
    'stripe_charge_dispute_updated',
    'stripe_charge_dispute_closed',
    'stripe_charge_dispute_funds_withdrawn'
  )
  AND (
    column_name IN ('id','created','type','data__object__id','data__object__reason','data__object__status','data__object__amount','data__object__currency','data__object__is_charge_refundable','data__object__charge','data__object__evidence_details__has_evidence','data__object__evidence_details__due_by','data__object__evidence_details__submission_count')
    OR column_name LIKE 'data__previous_attributes%'
  )
ORDER BY table_name, column_name;

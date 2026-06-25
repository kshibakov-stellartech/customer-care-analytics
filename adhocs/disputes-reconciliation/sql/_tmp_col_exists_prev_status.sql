SELECT table_name, column_name
FROM information_schema.columns
WHERE table_schema='data_bronze_stripe_prod'
  AND table_name IN (
    'stripe_charge_dispute_created',
    'stripe_charge_dispute_updated',
    'stripe_charge_dispute_closed',
    'stripe_charge_dispute_funds_withdrawn'
  )
  AND column_name IN ('data__previous_attributes__status','data__object__evidence_details__submission_count','data__object__evidence_details__has_evidence','data__object__evidence_details__due_by')
ORDER BY table_name, column_name;

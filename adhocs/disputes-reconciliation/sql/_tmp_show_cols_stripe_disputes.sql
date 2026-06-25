SELECT table_name, column_name, data_type
FROM information_schema.columns
WHERE table_schema='data_bronze_stripe_prod'
  AND table_name IN (
    'stripe_charge_dispute_created',
    'stripe_charge_dispute_updated',
    'stripe_charge_dispute_closed',
    'stripe_charge_dispute_funds_withdrawn'
  )
ORDER BY table_name, ordinal_position;

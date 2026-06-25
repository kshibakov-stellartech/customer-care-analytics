WITH t AS (
  SELECT * FROM (VALUES
    ('stripe_charge_dispute_created'),
    ('stripe_charge_dispute_updated'),
    ('stripe_charge_dispute_closed'),
    ('stripe_charge_dispute_funds_withdrawn'),
    ('stripe_charge_dispute_funds_reinstated'),
    ('stripe_radar_early_fraud_warning_created'),
    ('stripe_radar_early_fraud_warning_updated'),
    ('stripe_charge_succeeded'),
    ('stripe_charge_failed'),
    ('stripe_charge_captured')
  ) AS x(table_name)
)
SELECT c.table_name, c.column_name
FROM information_schema.columns c
JOIN t ON c.table_name = t.table_name
WHERE c.table_schema = 'data_bronze_stripe_prod'
  AND (
    lower(c.column_name) LIKE '%email%'
    OR lower(c.column_name) LIKE '%charge%'
    OR lower(c.column_name) LIKE '%dispute%'
    OR lower(c.column_name) LIKE '%fraud%'
    OR lower(c.column_name) LIKE '%risk%'
    OR lower(c.column_name) LIKE '%warning%'
    OR lower(c.column_name) LIKE '%reason%'
    OR lower(c.column_name) LIKE '%status%'
    OR lower(c.column_name) LIKE '%payment_intent%'
    OR lower(c.column_name) = 'id'
    OR lower(c.column_name) = 'created'
  )
ORDER BY c.table_name, c.column_name;

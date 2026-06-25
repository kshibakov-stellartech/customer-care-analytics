SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema='fivetran_paypal_prod' AND table_name='evidence'
ORDER BY ordinal_position;

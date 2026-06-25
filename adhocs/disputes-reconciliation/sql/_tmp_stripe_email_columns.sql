SELECT table_schema, table_name, column_name
FROM information_schema.columns
WHERE table_schema = 'data_bronze_stripe_prod'
  AND lower(column_name) LIKE '%email%'
ORDER BY table_name, column_name;

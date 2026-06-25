SELECT table_name, column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'data_bronze_stripe_prod'
  AND table_name LIKE 'stripe_%'
  AND table_name NOT LIKE 'temp_table_%'
  AND (
    lower(column_name) = 'customer'
    OR lower(column_name) = 'data__object__customer'
    OR lower(column_name) like '%__customer'
    OR lower(column_name) like '%__customer_id'
    OR lower(column_name) like 'customer%'
  )
ORDER BY table_name, column_name;

SELECT DISTINCT table_name
FROM information_schema.columns
WHERE table_schema = 'data_bronze_stripe_prod'
  AND (
    lower(table_name) LIKE '%fraud%'
    OR lower(table_name) LIKE '%dispute%'
    OR lower(column_name) LIKE '%fraud%'
    OR lower(column_name) LIKE '%dispute%'
    OR lower(column_name) LIKE '%risk%'
    OR lower(column_name) LIKE '%warning%'
  )
  AND lower(table_name) NOT LIKE 'temp_table_%'
ORDER BY 1;

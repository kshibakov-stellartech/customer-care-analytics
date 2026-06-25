SELECT table_name, column_name
FROM information_schema.columns
WHERE table_schema = 'data_bronze_stripe_prod'
  AND (
    lower(table_name) LIKE '%fraud%'
    OR lower(table_name) LIKE '%dispute%'
    OR lower(table_name) LIKE '%warning%'
    OR lower(table_name) LIKE '%risk%'
    OR lower(column_name) LIKE '%fraud%'
    OR lower(column_name) LIKE '%dispute%'
    OR lower(column_name) LIKE '%warning%'
    OR lower(column_name) LIKE '%risk%'
  )
ORDER BY table_name, column_name;

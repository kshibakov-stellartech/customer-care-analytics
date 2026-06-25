SELECT table_name, column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'fivetran_asana'
  AND (
    lower(table_name) LIKE '%task%'
    OR lower(table_name) LIKE '%section%'
    OR lower(table_name) LIKE '%story%'
    OR lower(table_name) LIKE '%event%'
  )
ORDER BY table_name, ordinal_position;

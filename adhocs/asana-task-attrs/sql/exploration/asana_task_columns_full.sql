SELECT column_name, data_type, ordinal_position
FROM information_schema.columns
WHERE table_schema = 'fivetran_asana'
  AND table_name = 'task'
ORDER BY ordinal_position;

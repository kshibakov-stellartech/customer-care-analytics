SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'fivetran_asana'
  AND table_name = 'story'
ORDER BY ordinal_position;

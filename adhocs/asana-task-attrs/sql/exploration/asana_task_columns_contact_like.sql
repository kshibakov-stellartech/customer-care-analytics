SELECT column_name
FROM information_schema.columns
WHERE table_schema = 'fivetran_asana'
  AND table_name = 'task'
  AND (
    lower(column_name) LIKE '%email%'
    OR lower(column_name) LIKE '%mail%'
    OR lower(column_name) LIKE '%ticket%'
    OR lower(column_name) LIKE '%zendesk%'
    OR lower(column_name) LIKE '%link%'
    OR lower(column_name) LIKE '%url%'
  )
ORDER BY column_name;

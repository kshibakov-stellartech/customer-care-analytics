SELECT column_name
FROM information_schema.columns
WHERE table_schema = 'fivetran_asana'
  AND table_name = 'task'
  AND (
    lower(column_name) LIKE '%email%'
    OR lower(column_name) LIKE '%mail%'
    OR lower(column_name) LIKE '%address%'
    OR lower(column_name) LIKE '%contact%'
    OR lower(column_name) LIKE '%requester%'
    OR lower(column_name) LIKE '%user%'
    OR lower(column_name) LIKE '%profile%'
  )
ORDER BY column_name;

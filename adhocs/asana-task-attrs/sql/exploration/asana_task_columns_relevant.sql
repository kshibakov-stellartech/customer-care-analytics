SELECT column_name
FROM information_schema.columns
WHERE table_schema = 'fivetran_asana'
  AND table_name = 'task'
  AND (
    lower(column_name) LIKE '%email%'
    OR lower(column_name) LIKE '%ticket%'
    OR lower(column_name) LIKE '%request%'
    OR lower(column_name) LIKE '%source%'
    OR lower(column_name) LIKE '%user%'
    OR lower(column_name) LIKE '%chat%'
    OR lower(column_name) LIKE '%profile%'
    OR lower(column_name) LIKE '%adapty%'
    OR lower(column_name) LIKE '%details%'
  )
ORDER BY column_name;

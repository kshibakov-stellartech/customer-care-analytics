SELECT
  table_schema,
  table_name,
  column_name,
  data_type
FROM information_schema.columns
WHERE table_schema = 'data_bronze_stripe_prod'
  AND (
    regexp_like(lower(column_name), '(^|_)email($|_)')
    OR regexp_like(lower(column_name), '(^|_)customer($|_)')
    OR regexp_like(lower(column_name), '(^|_)phone($|_)')
    OR regexp_like(lower(column_name), '(^|_)name($|_)')
    OR regexp_like(lower(column_name), '(^|_)user($|_)')
    OR regexp_like(lower(column_name), '(^|_)billing($|_)')
    OR regexp_like(lower(column_name), '(^|_)shipping($|_)')
    OR regexp_like(lower(column_name), '(^|_)address($|_)')
    OR regexp_like(lower(column_name), '(^|_)fingerprint($|_)')
    OR regexp_like(lower(column_name), '(^|_)ip($|_)')
    OR regexp_like(lower(column_name), '(^|_)metadata($|_)')
    OR regexp_like(lower(column_name), '(^|_)receipt($|_)')
  )
ORDER BY table_name, column_name;

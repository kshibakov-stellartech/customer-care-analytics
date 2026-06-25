SELECT
  table_name,
  column_name
FROM information_schema.columns
WHERE table_schema = 'data_bronze_stripe_prod'
  AND table_name LIKE 'stripe_%'
  AND table_name NOT LIKE 'temp_table_%'
  AND (
    regexp_like(lower(column_name), '(^|__)email($|__)')
    OR regexp_like(lower(column_name), '(^|__)customer($|__)')
    OR regexp_like(lower(column_name), '(^|__)customer_id($|__)')
    OR regexp_like(lower(column_name), '(^|__)phone($|__)')
    OR regexp_like(lower(column_name), '(^|__)name($|__)')
    OR regexp_like(lower(column_name), '(^|__)billing_details($|__)')
    OR regexp_like(lower(column_name), '(^|__)shipping($|__)')
    OR regexp_like(lower(column_name), '(^|__)address($|__)')
    OR regexp_like(lower(column_name), '(^|__)fingerprint($|__)')
    OR regexp_like(lower(column_name), '(^|__)customer_purchase_ip($|__)')
    OR regexp_like(lower(column_name), '(^|__)metadata__customer_email($|__)')
    OR regexp_like(lower(column_name), '(^|__)metadata__customer_id($|__)')
  )
ORDER BY table_name, column_name;

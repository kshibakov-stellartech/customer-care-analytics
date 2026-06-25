SELECT
  table_name,
  column_name,
  CASE
    WHEN regexp_like(lower(column_name), 'email') THEN 'email'
    WHEN regexp_like(lower(column_name), 'customer') THEN 'customer'
    WHEN regexp_like(lower(column_name), 'phone') THEN 'phone'
    WHEN regexp_like(lower(column_name), 'name') THEN 'name'
    WHEN regexp_like(lower(column_name), 'address|shipping|billing') THEN 'address_or_profile'
    WHEN regexp_like(lower(column_name), 'fingerprint') THEN 'fingerprint'
    WHEN regexp_like(lower(column_name), 'ip') THEN 'ip'
    WHEN regexp_like(lower(column_name), 'metadata') THEN 'metadata'
    ELSE 'other'
  END AS id_type
FROM information_schema.columns
WHERE table_schema = 'data_bronze_stripe_prod'
  AND table_name LIKE 'stripe_%'
  AND table_name NOT LIKE 'temp_table_%'
  AND regexp_like(lower(column_name), 'email|customer|phone|name|address|shipping|billing|fingerprint|ip|metadata')
ORDER BY table_name, column_name;

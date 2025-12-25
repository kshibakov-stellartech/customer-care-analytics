SELECT *--schema_name, table_name, column_name
FROM data_bronze_supabase_prod.smartyme_auth__users
WHERE 1=1
  AND raw_user_meta_data__email = 'monie1770@gmail.com'
  --AND id = '65729490-6d10-4df4-8728-49ba9ba417a0'
LIMIT 10;

SELECT
  query_id,
  workgroup,
  data_scanned_in_bytes,
  result_reuse_info.reused_previous_result
FROM system.runtime.queries
ORDER BY submission_date DESC
LIMIT 10;

WITH tbl AS (
SELECT json_extract_scalar(attributes, '$.email') as raw_email,
       json_extract_scalar(attributes, '$.user_id') as raw_user_id,
       json_extract_scalar(attributes, '$.authUserId') as raw_authUserId,
       json_extract_scalar(attributes, '$.sf_user_id') as raw_sf_user_id,
       *
FROM data_bronze_adapty_prod.adapty_events_export aee
WHERE 1=1
  --AND email IN ('mmartin@weoneil.com', 'monie1770@gmail.com')
--AzqKFTUixYF38CuPY
  --AND json_extract_scalar(attributes, '$.email') IN ('mmartin@weoneil.com', 'monie1770@gmail.com')
)

SELECT *
FROM tbl
WHERE 1=1
  --AND raw_email IN ('mmartin@weoneil.com', 'monie1770@gmail.com')
  AND profile_id = 'ac4779fd-415c-476d-b429-86de0db86a2c'
LIMIT 100
;


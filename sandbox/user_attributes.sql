/*
app_user_id
bo_user_id
supabase_user_id
zendesk_user_id
auth_user_email
contact_user_email
reteno_id
*/

SELECT 'app_user_id' as user_attribute,
       events__value as value
FROM data_bronze_zendesk_prod.zendesk_audit
WHERE ticket_id = 584787
  AND events__field_name = '40831328206865'
UNION ALL

SELECT 'bo_user_id' as user_attribute,
       events__value as value
FROM data_bronze_zendesk_prod.zendesk_audit
WHERE ticket_id = 584787
  AND events__field_name = '32351109113361'
UNION ALL

/*
здесь не получили id supabase, но в базе супабейза нашелся через mail
*/
SELECT 'supabase_user_id' as user_attribute,
       events__value as value
FROM data_bronze_zendesk_prod.zendesk_audit
WHERE ticket_id = 584787
  AND events__field_name = '32351085497873'
UNION ALL

SELECT 'zendesk_user_id' as user_attribute,
       events__value as value
FROM data_bronze_zendesk_prod.zendesk_audit
WHERE ticket_id = 584787
  AND events__field_name = 'requester_id'
UNION ALL

SELECT 'reteno_user_id' as user_attribute,
       events__value as value
FROM data_bronze_zendesk_prod.zendesk_audit
WHERE ticket_id = 584787
  AND events__field_name = '30908934762257'
UNION ALL

SELECT 'auth_user_email' as user_attribute,
       events__value as value
FROM data_bronze_zendesk_prod.zendesk_audit
WHERE ticket_id = 584787
  AND events__field_name = '30971824463761'
UNION ALL

SELECT 'contact_user_email' as user_attribute,
       events__value as value
FROM data_bronze_zendesk_prod.zendesk_audit
WHERE ticket_id = 584787
  AND events__field_name = '30971823749777'


-- 43f4883d-806a-42d2-86ee-6c9e99a03363

SELECT id, target_id, created_at, type, text
FROM fivetran_asana.story
WHERE lower(coalesce(text, '')) LIKE '%moved%'
   OR lower(coalesce(text, '')) LIKE '%section%'
ORDER BY created_at DESC
LIMIT 200;

SELECT id, created_at, type, text, created_by_id, source
FROM fivetran_asana.story
WHERE target_id = '1212082488015300'
ORDER BY created_at;

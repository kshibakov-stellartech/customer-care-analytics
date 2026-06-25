SELECT id, task_id, created_at, type, resource_subtype, text
FROM fivetran_asana.story
WHERE (
    lower(coalesce(type, '')) LIKE '%section%'
    OR lower(coalesce(resource_subtype, '')) LIKE '%section%'
    OR lower(coalesce(text, '')) LIKE '%moved%'
    OR lower(coalesce(text, '')) LIKE '%section%'
  )
ORDER BY created_at DESC
LIMIT 100;

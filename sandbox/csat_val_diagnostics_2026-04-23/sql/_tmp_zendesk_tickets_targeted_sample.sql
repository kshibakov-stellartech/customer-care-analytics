SELECT
  CAST(created_at AS DATE) AS dt,
  ticket_id,
  subject,
  description,
  voc_category,
  CARDINALITY(tags) AS tags_cnt,
  element_at(tags, 1) AS tag1,
  element_at(tags, 2) AS tag2
FROM data_bronze_zendesk_prod.zendesk_tickets
WHERE CAST(created_at AS DATE) >= DATE '2025-11-01'
ORDER BY created_at DESC
LIMIT 20;

WITH s AS (
  SELECT
    CAST(created_at AS DATE) AS dt,
    ticket_id,
    voc_category,
    tags
  FROM data_bronze_zendesk_prod.zendesk_tickets
  WHERE CAST(created_at AS DATE) >= DATE '2025-11-01'
)
SELECT
  COUNT(*) AS rows_total,
  COUNT_IF(voc_category IS NOT NULL AND TRIM(voc_category) <> '') AS rows_with_voc_category,
  COUNT_IF(CARDINALITY(tags) > 0) AS rows_with_any_tags,
  COUNT_IF(CARDINALITY(FILTER(tags, t -> LOWER(t) LIKE 'voc\_%')) > 0) AS rows_with_voc_tag,
  COUNT_IF((voc_category IS NULL OR TRIM(voc_category) = '') AND CARDINALITY(FILTER(tags, t -> LOWER(t) LIKE 'voc\_%')) > 0) AS rows_only_voc_tag
FROM s;

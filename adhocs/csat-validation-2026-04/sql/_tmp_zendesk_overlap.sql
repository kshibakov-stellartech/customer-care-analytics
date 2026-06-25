WITH audit_voc AS (
  SELECT DISTINCT ticket_id
  FROM data_bronze_zendesk_prod.zendesk_audit
  CROSS JOIN UNNEST(SPLIT(events__value, ',')) AS u(tag)
  WHERE events__field_name = 'tags'
    AND created_at >= DATE '2025-11-01'
    AND REGEXP_LIKE(LOWER(TRIM(tag)), '^voc_')
),
tickets_voc AS (
  SELECT DISTINCT ticket_id
  FROM data_bronze_zendesk_prod.zendesk_tickets
  WHERE CAST(created_at AS DATE) >= DATE '2025-11-01'
    AND voc_category IS NOT NULL
    AND TRIM(voc_category) <> ''
)
SELECT
  (SELECT COUNT(*) FROM audit_voc) AS audit_voc_tickets,
  (SELECT COUNT(*) FROM tickets_voc) AS tickets_voc_tickets,
  (SELECT COUNT(*) FROM tickets_voc t LEFT JOIN audit_voc a ON t.ticket_id=a.ticket_id WHERE a.ticket_id IS NULL) AS tickets_only,
  (SELECT COUNT(*) FROM tickets_voc t JOIN audit_voc a ON t.ticket_id=a.ticket_id) AS overlap;

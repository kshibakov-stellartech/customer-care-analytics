SELECT *
FROM data_bronze_zendesk_prod.zendesk_tickets
WHERE CAST(created_at AS DATE) >= DATE '2025-11-01'
ORDER BY created_at DESC
LIMIT 5;

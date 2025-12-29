WITH raw_events AS (
    SELECT *
    FROM data_silver_zendesk_prod.zendesk_events
    WHERE 1=1
      --AND event_at >= DATE '2025-11-01'
      AND ticket_id = 652098
      --AND (lower(event_type) LIKE '%chat%' OR lower(event_type) LIKE '%status%' OR lower(event_type) LIKE '%reply%')
      --AND event_type = 'status_change'
    ORDER BY event_at
),


SELECT COUNT(DISTINCT event_at) as action_times
FROM raw_events
--GROUP BY 1, 2
--ORDER BY 1

;

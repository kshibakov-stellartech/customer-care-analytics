WITH
excluded_tag_patterns AS (
    SELECT *
    FROM (
        VALUES
            ('%cancellation_notification%'),
            ('%closed_by_merge%'),
            ('%voice_abandoned_in_voicemail%'),
            ('%appfollow%'),
            ('%spam%'),
            ('%ai_cb_triggered%'),
            ('%chargeback_precom%'),
            ('%chargeback_postcom%')
    ) AS t(pattern)
),

tickets_to_exclude AS (
    SELECT
        za.ticket_id AS ticket_to_exclude_id,
        MIN(CAST(za.created_at AS DATE)) AS created_date
    FROM data_bronze_zendesk_prod.zendesk_audit za
    JOIN excluded_tag_patterns etp
      ON za.events__field_name = 'tags'
     AND za.events__value LIKE etp.pattern
    WHERE za.created_at >= DATE '2026-05-22'
    GROUP BY 1
),

tickets AS (
    SELECT
        za.ticket_id,
        MIN(za.created_at) AS ticket_created_at,
        CAST(MAX(za.events__value) AS BIGINT) AS requester_id
    FROM data_bronze_zendesk_prod.zendesk_audit za
    WHERE za.events__type = 'Create'
      AND za.events__field_name = 'requester_id'
      AND NOT EXISTS (
          SELECT 1
          FROM tickets_to_exclude te
          WHERE te.ticket_to_exclude_id = za.ticket_id
      )
    GROUP BY 1
    HAVING MIN(CAST(za.created_at AS DATE)) >= DATE '2026-05-22'
       AND MIN(CAST(za.created_at AS DATE)) < current_date
),
    trustpilot_tickets AS (
SELECT DATE_TRUNC('day', created_at) as dt,
       COUNT(DISTINCT ticket_id) as tickets_eligible,
       COUNT(DISTINCT CASE WHEN za.events__type = 'Comment' AND
                                za.events__public = TRUE AND
                                lower(events__body) LIKE '%trustpilot%'
                           THEN ticket_id
       END) as tickets_link_sent
FROM data_bronze_zendesk_prod.zendesk_audit za
    JOIN tickets t USING (ticket_id)
WHERE 1=1
  AND   (
      (
        za.events__type = 'Comment' AND
        za.events__public = TRUE AND
        lower(events__body) LIKE '%trustpilot%'
      )
        OR
       za.events__field_name = 'tags' AND
       za.events__value LIKE '%tp_opportunity_lost%'
        )
GROUP BY 1
),
    trustpilot_data AS (
SELECT  DATE_TRUNC('day', created_at) as dt,
        COUNT(DISTINCT review_id) as reviews_count
FROM data_silver_trustpilot_prod.trustpilot_reviews
GROUP BY 1
)

SELECT CAST(dt AS DATE) as dt,
       tickets_eligible,
       COALESCE(tickets_link_sent, 0) as tickets_link_sent,
       COALESCE(reviews_count, 0) as reviews_count
FROM trustpilot_tickets
    LEFT JOIN trustpilot_data USING (dt)
ORDER BY dt
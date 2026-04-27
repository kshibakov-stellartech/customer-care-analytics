WITH
tag_rows AS (
    SELECT
        ticket_id,
        CAST(created_at AS DATE) AS dt,
        LOWER(TRIM(tag)) AS tag_raw
    FROM data_bronze_zendesk_prod.zendesk_audit
    CROSS JOIN UNNEST(SPLIT(events__value, ',')) AS u(tag)
    WHERE created_at >= DATE '2025-11-01'
      AND events__field_name = 'tags'
),

ticket_users AS (
    SELECT
        ticket_id,
        MAX(events__value) AS user_id
    FROM data_bronze_zendesk_prod.zendesk_audit
    WHERE created_at >= DATE '2025-11-01'
      AND events__field_name IN ('32351109113361', '40831328206865', '32351085497873')
    GROUP BY 1
),

normalized AS (
    SELECT
        ticket_id,
        dt,
        REGEXP_REPLACE(tag_raw, '^voc_', '') AS base_tag
    FROM tag_rows
    WHERE REGEXP_LIKE(tag_raw, '^voc_')
),

ticket_voc_tag AS (
    SELECT
        ticket_id,
        MIN(dt) AS dt,
        MIN(base_tag) AS review
    FROM normalized
    GROUP BY 1
),

zendesk_source_from_audit AS (
    SELECT
        t.dt AS date,
        'zendesk' AS source,
        z.description AS text,
        t.review,
        CAST(t.ticket_id AS VARCHAR) AS source_id,
        u.user_id,
        'audit' AS zendesk_branch
    FROM ticket_voc_tag t
    LEFT JOIN data_bronze_zendesk_prod.zendesk_tickets z ON t.ticket_id = z.ticket_id
    LEFT JOIN ticket_users u ON t.ticket_id = u.ticket_id
),

zendesk_tickets_voc_tag AS (
    SELECT
        z.ticket_id,
        MIN(CAST(z.created_at AS DATE)) AS dt,
        MIN(REGEXP_REPLACE(LOWER(TRIM(z.voc_category)), '^voc_', '')) AS review,
        MAX(z.description) AS description
    FROM data_bronze_zendesk_prod.zendesk_tickets z
    WHERE CAST(z.created_at AS DATE) >= DATE '2025-11-01'
      AND CAST(z.created_at AS DATE) <= COALESCE((SELECT MAX(dt) FROM tag_rows), CURRENT_DATE)
      AND z.voc_category IS NOT NULL
      AND TRIM(z.voc_category) <> ''
    GROUP BY 1
),

zendesk_source_from_tickets AS (
    SELECT
        t.dt AS date,
        'zendesk' AS source,
        t.description AS text,
        t.review,
        CAST(t.ticket_id AS VARCHAR) AS source_id,
        u.user_id,
        'tickets' AS zendesk_branch
    FROM zendesk_tickets_voc_tag t
    LEFT JOIN ticket_users u ON t.ticket_id = u.ticket_id
    LEFT JOIN ticket_voc_tag a ON t.ticket_id = a.ticket_id
    WHERE a.ticket_id IS NULL
),

zendesk_source AS (
    SELECT * FROM zendesk_source_from_audit
    UNION ALL
    SELECT * FROM zendesk_source_from_tickets
)

SELECT
    zendesk_branch,
    COUNT(*) AS rows_cnt,
    COUNT_IF(date IS NULL) AS date_nulls,
    COUNT_IF(review IS NULL OR TRIM(review) = '') AS review_empty_or_null,
    COUNT_IF(source_id IS NULL OR TRIM(source_id) = '') AS source_id_empty_or_null,
    COUNT_IF(text IS NULL OR TRIM(text) = '') AS text_empty_or_null,
    COUNT_IF(user_id IS NULL OR TRIM(user_id) = '') AS user_id_empty_or_null,
    COUNT_IF(user_id IS NOT NULL AND TRIM(user_id) <> '') AS user_id_filled
FROM zendesk_source
GROUP BY 1
ORDER BY 1;

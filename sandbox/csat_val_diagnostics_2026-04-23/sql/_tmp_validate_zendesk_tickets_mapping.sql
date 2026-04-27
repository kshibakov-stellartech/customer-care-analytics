WITH
-- same upper bound as prod script
max_audit_dt AS (
    SELECT MAX(CAST(created_at AS DATE)) AS max_dt
    FROM data_bronze_zendesk_prod.zendesk_audit
    WHERE created_at >= DATE '2025-11-01'
      AND events__field_name = 'tags'
),

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

raw_tickets AS (
    SELECT
        z.ticket_id,
        CAST(z.created_at AS DATE) AS created_dt,
        z.description,
        z.voc_category
    FROM data_bronze_zendesk_prod.zendesk_tickets z
    CROSS JOIN max_audit_dt m
    WHERE CAST(z.created_at AS DATE) >= DATE '2025-11-01'
      AND CAST(z.created_at AS DATE) <= COALESCE(m.max_dt, CURRENT_DATE)
      AND z.voc_category IS NOT NULL
      AND TRIM(z.voc_category) <> ''
),

dedup_raw AS (
    SELECT r.*
    FROM raw_tickets r
    LEFT JOIN ticket_voc_tag a ON r.ticket_id = a.ticket_id
    WHERE a.ticket_id IS NULL
),

mapped AS (
    SELECT
        ticket_id,
        MIN(created_dt) AS date,
        MIN(REGEXP_REPLACE(LOWER(TRIM(voc_category)), '^voc_', '')) AS review,
        MAX(description) AS text,
        CAST(ticket_id AS VARCHAR) AS source_id
    FROM dedup_raw
    GROUP BY 1
)

SELECT
    (SELECT COUNT(*) FROM dedup_raw) AS raw_rows_after_dedup,
    (SELECT COUNT(DISTINCT ticket_id) FROM dedup_raw) AS raw_distinct_tickets_after_dedup,
    (SELECT COUNT(*) FROM mapped) AS mapped_rows,
    (SELECT COUNT(*) FROM mapped WHERE date IS NULL) AS mapped_date_nulls,
    (SELECT COUNT(*) FROM mapped WHERE review IS NULL) AS mapped_review_nulls,
    (SELECT COUNT(*) FROM mapped WHERE TRIM(COALESCE(review, '')) = '') AS mapped_review_empty,
    (SELECT COUNT(*) FROM mapped WHERE source_id IS NULL) AS mapped_source_id_nulls,
    (SELECT COUNT(*) FROM mapped WHERE text IS NULL) AS mapped_text_nulls,
    (SELECT COUNT(*) FROM mapped WHERE TRIM(COALESCE(text, '')) = '') AS mapped_text_empty,
    (SELECT COUNT(DISTINCT ticket_id) FROM dedup_raw WHERE description IS NOT NULL AND TRIM(description) <> '') AS raw_tickets_with_nonempty_text,
    (SELECT COUNT(*) FROM mapped WHERE text IS NOT NULL AND TRIM(text) <> '') AS mapped_tickets_with_nonempty_text;

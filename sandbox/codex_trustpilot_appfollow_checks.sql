-- Codex checks for Trustpilot + AppFollow reload validation
-- Relevant sources:
--   1) data_silver_appfollow_prod.appfollow_reviews
--   2) data_silver_trustpilot_prod.trustpilot_reviews
-- Task:
--   Determine the active period where voc_category IS NOT NULL
--   and build monthly dynamics for such rows.

WITH categorized_reviews AS (
    SELECT
        'appfollow' AS source,
        CAST(COALESCE(appfollow_collected_datetime, review_published_datetime) AS DATE) AS event_dt,
        review_id,
        voc_category
    FROM data_silver_appfollow_prod.appfollow_reviews
    WHERE TRIM(COALESCE(voc_category, '')) <> ''

    UNION ALL

    SELECT
        'trustpilot' AS source,
        CAST(created_at AS DATE) AS event_dt,
        review_id,
        voc_category
    FROM data_silver_trustpilot_prod.trustpilot_reviews
    WHERE TRIM(COALESCE(voc_category, '')) <> ''
)

-- 1. Period with non-null voc_category by source
SELECT
    source,
    MIN(event_dt) AS period_start_dt,
    MAX(event_dt) AS period_end_dt,
    DATE_DIFF('day', MIN(event_dt), MAX(event_dt)) + 1 AS period_days,
    COUNT(*) AS categorized_row_cnt,
    COUNT(DISTINCT review_id) AS distinct_review_id_cnt
FROM categorized_reviews
GROUP BY 1
ORDER BY 1;

-- 2. Monthly dynamics for rows with non-null voc_category
WITH categorized_reviews AS (
    SELECT
        'appfollow' AS source,
        CAST(COALESCE(appfollow_collected_datetime, review_published_datetime) AS DATE) AS event_dt,
        review_id,
        voc_category
    FROM data_silver_appfollow_prod.appfollow_reviews
    WHERE TRIM(COALESCE(voc_category, '')) <> ''

    UNION ALL

    SELECT
        'trustpilot' AS source,
        CAST(created_at AS DATE) AS event_dt,
        review_id,
        voc_category
    FROM data_silver_trustpilot_prod.trustpilot_reviews
    WHERE TRIM(COALESCE(voc_category, '')) <> ''
)
SELECT
    source,
    CAST(DATE_TRUNC('month', event_dt) AS DATE) AS month_dt,
    COUNT(*) AS categorized_row_cnt,
    COUNT(DISTINCT review_id) AS distinct_review_id_cnt
FROM categorized_reviews
GROUP BY 1, 2
ORDER BY month_dt, source;

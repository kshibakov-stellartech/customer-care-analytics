WITH
    trustpilot_data AS (
SELECT  created_at AS review_published_datetime,
        created_at AS appfollow_collected_datetime,
        'Trustpilot' store,
        lower(country) as country,
        reviewer_name as author,
        review_id,
        title,
        text as content,
        null as content_translated,
        rating,
        CASE WHEN rating >= 4 THEN 'Positive review' ELSE COALESCE(ai_category, 'No details')  END as ai_category,
        ai_category as ai_category_raw
FROM data_silver_trustpilot_prod.trustpilot_reviews
),
    appfollow_data AS (
SELECT  review_published_datetime,
        appfollow_collected_datetime,
        store,
        lower(country) as country,
        author,
        review_id,
        title,
        content,
        content_translated,
        rating,
        CASE WHEN rating >= 4 THEN 'Positive review' ELSE COALESCE(ai_category, 'No details')  END as ai_category,
        ai_category as ai_category_raw
FROM data_silver_appfollow_prod.appfollow_reviews
),
    all_sources AS (
SELECT *,
       CAST(DATE_TRUNC('week', review_published_datetime) AS DATE) as review_week_dt,
       CAST(DATE_TRUNC('week', appfollow_collected_datetime) AS DATE) as load_week_dt
FROM appfollow_data
UNION ALL
SELECT *,
       CAST(DATE_TRUNC('week', review_published_datetime) AS DATE) as review_week_dt,
       CAST(DATE_TRUNC('week', appfollow_collected_datetime) AS DATE) as load_week_dt
FROM trustpilot_data
),
    reviews_normalized AS (
SELECT *,
       /* ---------- MAIN CATEGORY ---------- */
       CASE
            WHEN ai_category IS NULL OR trim(ai_category) = '' THEN NULL

            /* priority bucket -> always Misleading payment practices */
            WHEN regexp_like(ai_category, '(?i)misleading payment practices')
              OR regexp_like(ai_category, '(?i)misleading payment')
              OR regexp_like(ai_category, '(?i)scam')
            THEN 'Misleading payment practices'

            /* otherwise: take first part before '+' and drop suffix after '_' */
            ELSE
                concat(
                    upper(substr(trim(regexp_replace(trim(split_part(ai_category, '+', 1)), '_.*', '')), 1, 1)),
                    substr(trim(regexp_replace(trim(split_part(ai_category, '+', 1)), '_.*', '')), 2)
                )
       END AS main_category,

    /* ---------- SUB CATEGORY (only for Misleading) ---------- */
        CASE
            WHEN ai_category IS NULL OR trim(ai_category) = '' THEN NULL

            WHEN regexp_like(ai_category, '(?i)misleading payment practices')
              OR regexp_like(ai_category, '(?i)misleading payment')
              OR regexp_like(ai_category, '(?i)scam')
            THEN
                concat(
                    upper(substr(trim(coalesce(
                        nullif(regexp_extract(ai_category, '(?i)misleading payment practices_([^+]+)', 1), ''),
                        'No details'
                    )), 1, 1)),
                    substr(trim(coalesce(
                        nullif(regexp_extract(ai_category, '(?i)misleading payment practices_([^+]+)', 1), ''),
                        'No details'
                    )), 2)
                )
            ELSE NULL
        END AS sub_category,
        CASE
            WHEN ai_category IS NULL OR trim(ai_category) = '' THEN NULL
            WHEN strpos(ai_category, '+') = 0 THEN NULL

            /* если misleading bucket — не создаём вторую категорию */
            WHEN regexp_like(ai_category, '(?i)misleading payment practices')
              OR regexp_like(ai_category, '(?i)misleading payment')
              OR regexp_like(ai_category, '(?i)scam')
            THEN NULL

            ELSE
                concat(
                    upper(substr(trim(regexp_replace(split_part(ai_category, '+', 2), '_.*', '')), 1, 1)),
                    substr(trim(regexp_replace(split_part(ai_category, '+', 2), '_.*', '')), 2)
                )
        END AS second_main_category
FROM all_sources
WHERE 1=1
  AND review_published_datetime < current_date
),
    full_review_history_uniq AS (
SELECT review_id,
       rating,
       content,
       content as text,
       MIN(observed_at) as observed_at
FROM data_silver_appfollow_prod.appfollow_reviews_history
GROUP BY 1, 2, 3
UNION ALL
SELECT review_id,
       rating,
       text as content,
       text,
       MIN(observed_at) as observed_at
FROM data_silver_trustpilot_prod.trustpilot_reviews_history
GROUP BY 1, 2, 3
),
    full_review_history AS (
SELECT review_id,
       rating,
       content,
       observed_at,
       CASE WHEN rating >= 4 THEN 'Positive review' ELSE COALESCE(ai_category, 'No details')  END as ai_category,
       ROW_NUMBER() OVER(PARTITION BY review_id ORDER BY observed_at) as version_num,
       COUNT(1) OVER(PARTITION BY review_id) as total_count
FROM data_silver_appfollow_prod.appfollow_reviews_history
    JOIN full_review_history_uniq USING (review_id, rating, content, observed_at)
UNION ALL
SELECT review_id,
       rating,
       text as content,
       observed_at,
       CASE WHEN rating >= 4 THEN 'Positive review' ELSE COALESCE(ai_category, 'No details')  END as ai_category,
       ROW_NUMBER() OVER(PARTITION BY review_id ORDER BY observed_at) as version_num,
       COUNT(1) OVER(PARTITION BY review_id) as total_count
FROM data_silver_trustpilot_prod.trustpilot_reviews_history
    JOIN full_review_history_uniq USING (review_id, rating, text, observed_at)
),
    updated_reviews AS (
SELECT review_id,
       rating as rating_old,
       content as content_old,
       ai_category as ai_category_old,
       /* ---------- MAIN CATEGORY ---------- */
       CASE
            WHEN ai_category IS NULL OR trim(ai_category) = '' THEN NULL

            /* priority bucket -> always Misleading payment practices */
            WHEN regexp_like(ai_category, '(?i)misleading payment practices')
              OR regexp_like(ai_category, '(?i)misleading payment')
              OR regexp_like(ai_category, '(?i)scam')
            THEN 'Misleading payment practices'

            /* otherwise: take first part before '+' and drop suffix after '_' */
            ELSE
                concat(
                    upper(substr(trim(regexp_replace(trim(split_part(ai_category, '+', 1)), '_.*', '')), 1, 1)),
                    substr(trim(regexp_replace(trim(split_part(ai_category, '+', 1)), '_.*', '')), 2)
                )
       END AS main_category_old,

    /* ---------- SUB CATEGORY (only for Misleading) ---------- */
        CASE
            WHEN ai_category IS NULL OR trim(ai_category) = '' THEN NULL

            WHEN regexp_like(ai_category, '(?i)misleading payment practices')
              OR regexp_like(ai_category, '(?i)misleading payment')
              OR regexp_like(ai_category, '(?i)scam')
            THEN
                concat(
                    upper(substr(trim(coalesce(
                        nullif(regexp_extract(ai_category, '(?i)misleading payment practices_([^+]+)', 1), ''),
                        'No details'
                    )), 1, 1)),
                    substr(trim(coalesce(
                        nullif(regexp_extract(ai_category, '(?i)misleading payment practices_([^+]+)', 1), ''),
                        'No details'
                    )), 2)
                )
            ELSE NULL
        END AS sub_category_old,
        CASE
            WHEN ai_category IS NULL OR trim(ai_category) = '' THEN NULL
            WHEN strpos(ai_category, '+') = 0 THEN NULL

            /* если misleading bucket — не создаём вторую категорию */
            WHEN regexp_like(ai_category, '(?i)misleading payment practices')
              OR regexp_like(ai_category, '(?i)misleading payment')
              OR regexp_like(ai_category, '(?i)scam')
            THEN NULL

            ELSE
                concat(
                    upper(substr(trim(regexp_replace(split_part(ai_category, '+', 2), '_.*', '')), 1, 1)),
                    substr(trim(regexp_replace(split_part(ai_category, '+', 2), '_.*', '')), 2)
                )
        END AS second_main_category_old,
       observed_at as updated_at
FROM full_review_history
WHERE 1=1
AND total_count > 1
AND version_num = 1

),
    res AS (
SELECT rn.*,
       CASE WHEN ur.updated_at IS NOT NULL THEN 1 ELSE 0 END as is_updated,
       ur.updated_at,
       ur.rating_old,
       ur.content_old,
       ur.ai_category_old,
       ur.main_category_old,
       ur.sub_category_old,
       ur.second_main_category_old
FROM reviews_normalized rn
    LEFT JOIN updated_reviews ur ON ur.review_id = rn.review_id
)

SELECT *,
       CASE WHEN content <> content_old  THEN 'text_update'
            WHEN rating  <> rating_old THEN 'rating_update'
            ELSE null
       END as diff_content
FROM res
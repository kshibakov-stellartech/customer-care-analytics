WITH normalized AS (
    SELECT
        review_id,
        has_answer,
        answer_published_datetime,
        answer_text,
        observed_at,
        row_hash,
        md5(to_utf8(
            coalesce(cast(answer_published_datetime AS varchar), '') || '|' ||
            coalesce(cast(has_answer AS varchar), '') || '|' ||
            coalesce(regexp_replace(trim(answer_text), '\\s+', ' '), '')
        )) AS reply_version_hash
    FROM data_silver_appfollow_prod.appfollow_reviews_history
    WHERE review_id = '56ad8f27-3259-4b6e-b4b4-7d5eb71f180a'
      AND has_answer = true
      AND answer_text IS NOT NULL
      AND trim(answer_text) <> ''
),
ranked AS (
    SELECT
        review_id,
        has_answer,
        answer_published_datetime,
        answer_text,
        observed_at,
        row_hash,
        reply_version_hash,
        row_number() OVER (
            PARTITION BY review_id, reply_version_hash
            ORDER BY observed_at DESC, row_hash DESC
        ) AS rn,
        min(observed_at) OVER (
            PARTITION BY review_id, reply_version_hash
        ) AS first_seen_at,
        max(observed_at) OVER (
            PARTITION BY review_id, reply_version_hash
        ) AS last_seen_at,
        count(*) OVER (
            PARTITION BY review_id, reply_version_hash
        ) AS source_rows_cnt
    FROM normalized
)
SELECT
    review_id,
    has_answer,
    answer_published_datetime,
    answer_text,
    first_seen_at,
    last_seen_at,
    source_rows_cnt,
    reply_version_hash
FROM ranked
WHERE rn = 1
ORDER BY answer_published_datetime, first_seen_at;

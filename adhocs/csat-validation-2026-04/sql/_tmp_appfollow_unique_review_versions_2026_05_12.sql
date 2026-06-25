WITH normalized AS (
    SELECT
        review_id,
        author,
        content,
        has_answer,
        observed_at,
        row_hash,
        md5(to_utf8(
            coalesce(regexp_replace(trim(author), '\\s+', ' '), '') || '|' ||
            coalesce(regexp_replace(trim(content), '\\s+', ' '), '') || '|' ||
            coalesce(cast(has_answer AS varchar), '')
        )) AS review_version_hash
    FROM data_silver_appfollow_prod.appfollow_reviews_history
    WHERE content IS NOT NULL
      AND trim(content) <> ''
),
ranked AS (
    SELECT
        review_id,
        author,
        content,
        has_answer,
        observed_at,
        row_hash,
        review_version_hash,
        row_number() OVER (
            PARTITION BY review_id, review_version_hash
            ORDER BY observed_at DESC, row_hash DESC
        ) AS rn,
        min(observed_at) OVER (
            PARTITION BY review_id, review_version_hash
        ) AS first_seen_at,
        max(observed_at) OVER (
            PARTITION BY review_id, review_version_hash
        ) AS last_seen_at,
        count(*) OVER (
            PARTITION BY review_id, review_version_hash
        ) AS source_rows_cnt
    FROM normalized
)
SELECT
    review_id,
    author,
    content,
    has_answer,
    first_seen_at,
    last_seen_at,
    source_rows_cnt
FROM ranked
WHERE rn = 1
ORDER BY review_id, last_seen_at;

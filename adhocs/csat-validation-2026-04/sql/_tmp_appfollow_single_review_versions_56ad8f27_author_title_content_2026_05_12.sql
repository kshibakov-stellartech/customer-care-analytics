WITH normalized AS (
    SELECT
        review_id,
        author,
        title,
        content,
        observed_at,
        row_hash,
        md5(to_utf8(
            coalesce(regexp_replace(trim(author), '\\s+', ' '), '') || '|' ||
            coalesce(regexp_replace(trim(title), '\\s+', ' '), '') || '|' ||
            coalesce(regexp_replace(trim(content), '\\s+', ' '), '')
        )) AS review_version_hash
    FROM data_silver_appfollow_prod.appfollow_reviews_history
    WHERE review_id = '56ad8f27-3259-4b6e-b4b4-7d5eb71f180a'
),
ranked AS (
    SELECT
        review_id,
        author,
        title,
        content,
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
  title,
  content,
  first_seen_at,
  last_seen_at,
  source_rows_cnt
FROM ranked
WHERE rn = 1
ORDER BY first_seen_at;

WITH unique_versions AS (
    SELECT
        review_id,
        answer_published_datetime,
        answer_text,
        min(observed_at) AS first_seen_at,
        max(observed_at) AS last_seen_at,
        count(*) AS source_rows_cnt,
        md5(to_utf8(
            coalesce(cast(answer_published_datetime AS varchar), '') || '|' ||
            coalesce(cast(has_answer AS varchar), '') || '|' ||
            coalesce(regexp_replace(trim(answer_text), '\\s+', ' '), '')
        )) AS reply_version_hash
    FROM data_silver_appfollow_prod.appfollow_reviews_history
    WHERE answer_text IS NOT NULL
      AND trim(answer_text) <> ''
    GROUP BY 1,2,3,has_answer
),
reviews_with_changes AS (
    SELECT review_id
    FROM unique_versions
    GROUP BY 1
    HAVING count(*) >= 2
)
SELECT
    u.review_id,
    u.answer_published_datetime,
    u.answer_text,
    u.first_seen_at,
    u.last_seen_at,
    u.source_rows_cnt
FROM unique_versions u
JOIN reviews_with_changes c
  ON u.review_id = c.review_id
ORDER BY u.review_id, u.answer_published_datetime, u.first_seen_at
LIMIT 200;

WITH base AS (
    SELECT
        review_id,
        observed_at,
        row_hash,
        md5(to_utf8(
            coalesce(regexp_replace(trim(author), '\\s+', ' '), '') || '|' ||
            coalesce(regexp_replace(trim(title), '\\s+', ' '), '') || '|' ||
            coalesce(regexp_replace(trim(content), '\\s+', ' '), '') || '|' ||
            coalesce(cast(rating AS varchar), '') || '|' ||
            coalesce(store, '') || '|' ||
            coalesce(cast(review_last_updated_datetime AS varchar), '') || '|' ||
            coalesce(regexp_replace(trim(answer_text), '\\s+', ' '), '') || '|' ||
            coalesce(cast(answer_published_datetime AS varchar), '')
        )) AS state_hash
    FROM data_silver_appfollow_prod.appfollow_reviews_history
),
ordered AS (
    SELECT
        *,
        lag(state_hash) OVER (
            PARTITION BY review_id
            ORDER BY observed_at, row_hash
        ) AS prev_state_hash
    FROM base
),
change_points AS (
    SELECT
        review_id,
        observed_at,
        CASE WHEN prev_state_hash IS NULL OR prev_state_hash <> state_hash THEN 1 ELSE 0 END AS is_changed
    FROM ordered
)
SELECT
    review_id,
    count(*) AS total_snapshots,
    count_if(is_changed = 1) AS unique_state_versions,
    count(*) - count_if(is_changed = 1) AS duplicate_snapshots
FROM change_points
GROUP BY 1
ORDER BY unique_state_versions DESC, total_snapshots DESC, review_id
LIMIT 200;

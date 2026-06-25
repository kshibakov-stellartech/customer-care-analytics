WITH unique_versions AS (
    SELECT
        review_id,
        company_reply_date,
        company_reply_text,
        min(observed_at) AS first_seen_at,
        max(observed_at) AS last_seen_at,
        count(*) AS source_rows_cnt,
        md5(to_utf8(
            coalesce(cast(company_reply_date AS varchar), '') || '|' ||
            coalesce(regexp_replace(trim(company_reply_text), '\\s+', ' '), '')
        )) AS reply_version_hash
    FROM data_silver_trustpilot_prod.trustpilot_reviews_history
    WHERE company_reply_text IS NOT NULL
      AND trim(company_reply_text) <> ''
    GROUP BY 1,2,3
),
reviews_with_changes AS (
    SELECT review_id
    FROM unique_versions
    GROUP BY 1
    HAVING count(*) >= 2
)
SELECT
    u.review_id,
    u.company_reply_date,
    u.company_reply_text,
    u.first_seen_at,
    u.last_seen_at,
    u.source_rows_cnt
FROM unique_versions u
JOIN reviews_with_changes c
  ON u.review_id = c.review_id
ORDER BY u.review_id, u.company_reply_date, u.first_seen_at
LIMIT 200;

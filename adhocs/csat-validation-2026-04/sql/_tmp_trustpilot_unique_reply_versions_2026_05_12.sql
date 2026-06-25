WITH normalized AS (
    SELECT
        review_id,
        company_reply_date,
        company_reply_text,
        observed_at,
        row_hash,
        md5(to_utf8(
            coalesce(cast(company_reply_date AS varchar), '') || '|' ||
            coalesce(regexp_replace(trim(company_reply_text), '\\s+', ' '), '')
        )) AS reply_version_hash
    FROM data_silver_trustpilot_prod.trustpilot_reviews_history
    WHERE company_reply_text IS NOT NULL
      AND trim(company_reply_text) <> ''
),
ranked AS (
    SELECT
        review_id,
        company_reply_date,
        company_reply_text,
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
    company_reply_date,
    company_reply_text,
    first_seen_at,
    last_seen_at,
    source_rows_cnt
FROM ranked
WHERE rn = 1
ORDER BY review_id, company_reply_date, last_seen_at;

WITH unique_versions AS (
    SELECT review_id,
           md5(to_utf8(
             coalesce(cast(company_reply_date AS varchar), '') || '|' ||
             coalesce(regexp_replace(trim(company_reply_text), '\\s+', ' '), '')
           )) AS reply_version_hash
    FROM data_silver_trustpilot_prod.trustpilot_reviews_history
    WHERE company_reply_text IS NOT NULL
      AND trim(company_reply_text) <> ''
    GROUP BY 1,2
), per_review AS (
    SELECT review_id, count(*) AS unique_reply_versions_cnt
    FROM unique_versions
    GROUP BY 1
)
SELECT
  count(*) AS reviews_with_any_reply,
  count_if(unique_reply_versions_cnt >= 2) AS reviews_with_2plus_versions,
  max(unique_reply_versions_cnt) AS max_versions_per_review
FROM per_review;

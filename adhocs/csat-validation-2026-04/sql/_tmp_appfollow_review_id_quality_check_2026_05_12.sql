WITH per_review AS (
  SELECT
    review_id,
    count(*) AS total_rows,
    count(DISTINCT coalesce(regexp_replace(trim(author), '\\s+', ' '), '')) AS distinct_author_cnt,
    count(DISTINCT coalesce(regexp_replace(trim(title), '\\s+', ' '), '')) AS distinct_title_cnt,
    count(DISTINCT coalesce(regexp_replace(trim(content), '\\s+', ' '), '')) AS distinct_content_cnt,
    count(DISTINCT
      coalesce(regexp_replace(trim(author), '\\s+', ' '), '') || '|' ||
      coalesce(regexp_replace(trim(title), '\\s+', ' '), '') || '|' ||
      coalesce(regexp_replace(trim(content), '\\s+', ' '), '')
    ) AS distinct_review_versions_cnt
  FROM data_silver_appfollow_prod.appfollow_reviews_history
  GROUP BY 1
)
SELECT
  count(*) AS total_review_ids,
  count_if(total_rows > 1) AS review_ids_with_multiple_rows,
  count_if(distinct_review_versions_cnt > 1) AS review_ids_with_changed_author_title_content,
  max(distinct_review_versions_cnt) AS max_versions_per_review_id
FROM per_review;

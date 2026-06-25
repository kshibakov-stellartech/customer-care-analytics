SELECT
  review_id,
  count(*) AS total_rows,
  count_if(answer_text IS NOT NULL AND trim(answer_text) <> '') AS rows_with_answer_text,
  count_if(has_answer = true) AS rows_has_answer_true,
  count_if(has_answer = false) AS rows_has_answer_false,
  count(DISTINCT coalesce(cast(answer_published_datetime AS varchar), '') || '|' || coalesce(regexp_replace(trim(answer_text), '\\s+', ' '), '')) AS distinct_answer_versions_by_text_and_dt
FROM data_silver_appfollow_prod.appfollow_reviews_history
WHERE review_id = '56ad8f27-3259-4b6e-b4b4-7d5eb71f180a'
GROUP BY 1;

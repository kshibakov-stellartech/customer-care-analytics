SELECT
  review_id,
  observed_at,
  row_hash,
  has_answer,
  answer_published_datetime,
  answer_text,
  author,
  rating,
  title,
  content
FROM data_silver_appfollow_prod.appfollow_reviews_history
WHERE review_id = '56ad8f27-3259-4b6e-b4b4-7d5eb71f180a'
ORDER BY observed_at;

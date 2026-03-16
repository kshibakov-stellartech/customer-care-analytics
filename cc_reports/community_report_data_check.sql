SELECT CAST(DATE_TRUNC('WEEK', review_published_datetime) AS DATE) AS week_dt,
       ai_category,
       store,
       rating,
       COUNT(1) as review_cnt
FROM data_silver_appfollow_prod.appfollow_reviews a
WHERE 1=1
  AND store = 'App Store'
  AND review_published_datetime >= DATE '2026-02-09'
  AND review_published_datetime < DATE '2026-02-16'
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 3, 5
;

SELECT review_id,
       review_published_datetime,
       store,
       rating,
       ai_category,
       title,
       content,
       internal_id,
       user_id
FROM data_silver_appfollow_prod.appfollow_reviews a
WHERE 1=1

AND store = 'App Store'
AND review_published_datetime >= DATE '2026-02-09'
AND review_published_datetime < DATE '2026-02-16'

AND rating IN (4, 5)
AND (ai_category <> 'Positive review' OR ai_category IS NULL)
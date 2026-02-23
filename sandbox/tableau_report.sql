-- Define the CTE for Trustpilot at the very top
WITH RankedTrustpilot AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY "review_id"
      ORDER BY
        CASE WHEN "company_reply_date" IS NOT NULL THEN 0 ELSE 1 END ASC,
        "created_at" DESC
    ) AS rn
  FROM
    "data_silver_trustpilot_prod"."trustpilot_reviews"
),
    report_data AS (
-- Part 1: Appfollow data with explicit casting
SELECT
  CAST("appfollow_reviews"."answer_text" AS VARCHAR) AS "answer_text",
  CAST("appfollow_reviews"."author" AS VARCHAR) AS "author",
  CAST("appfollow_reviews"."ai_category" AS VARCHAR) AS "category_response3",
  CAST("appfollow_reviews"."content" AS VARCHAR) AS "content",
  CAST(LOWER("appfollow_reviews"."country") AS VARCHAR) AS "country",
  CAST("appfollow_reviews"."review_published_datetime" AS TIMESTAMP) AS "created",
  CAST("appfollow_reviews"."appfollow_collected_datetime" AS TIMESTAMP) AS "datetime",
  CAST("appfollow_reviews"."response_time" AS BIGINT) AS "delta_created_resp",
  CAST("appfollow_reviews"."appfollow_collected_datetime" AS DATE) AS "event_date",
  CAST("appfollow_reviews"."rating" AS INTEGER) AS "rating",
  CAST("appfollow_reviews"."answer_published_datetime" AS TIMESTAMP) AS "reply_datetime",
  CAST("appfollow_reviews"."review_id" AS VARCHAR) AS "review_id",
  CAST("appfollow_reviews"."store" AS VARCHAR) AS "store",
  CAST("appfollow_reviews"."title" AS VARCHAR) AS "title"
FROM
  "data_silver_appfollow_prod"."appfollow_reviews" AS "appfollow_reviews"

UNION ALL

-- Part 2: Select from the CTE defined above
SELECT
  CAST("company_reply_text" AS VARCHAR) AS "answer_text",
  CAST("consumer_id" AS VARCHAR) AS "author",
  CAST(NULL AS VARCHAR) AS "category_response3",
  CAST("text" AS VARCHAR) AS "content",
  CAST(LOWER("country") AS VARCHAR) AS "country",
  CAST(NULL AS TIMESTAMP) AS "created",
  CAST("created_at" AS TIMESTAMP) AS "datetime",
  CAST(NULL AS BIGINT) AS "delta_created_resp",
  CAST("created_at" AS DATE) AS "event_date",
  CAST("rating" AS INTEGER) AS "rating",
  CAST("company_reply_date" AS TIMESTAMP) AS "reply_datetime",
  CAST("review_id" AS VARCHAR) AS "review_id",
  CAST('TrustPilot' AS VARCHAR) AS "store",
  CAST("title" AS VARCHAR) AS "title"
FROM
  RankedTrustpilot
WHERE
  rn = 1
)

SELECT CAST(DATE_TRUNC('WEEK', datetime) AS DATE) AS week_dt,
       store,
       category_response3,
       COUNT(review_id) as review_cnt,
       COUNT(DISTINCT review_id) as dist_review_cnt
FROM report_data
WHERE datetime >= DATE '2026-01-01'
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
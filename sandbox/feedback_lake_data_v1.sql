WITH
-- =========================
-- 1) Zendesk: вытаскиваем VOC tag на тикет
-- =========================
tag_rows AS (
    SELECT
        ticket_id,
        CAST(created_at AS DATE) AS dt,
        LOWER(TRIM(tag)) AS tag_raw
    FROM data_bronze_zendesk_prod.zendesk_audit
    CROSS JOIN UNNEST(SPLIT(events__value, ',')) AS u(tag)
    WHERE 1=1
      AND created_at >= DATE '2026-01-01'
      AND events__field_name = 'tags'
),

normalized AS (
    SELECT
        ticket_id,
        dt,
        tag_raw,
        REGEXP_REPLACE(tag_raw, '^voc_', '') AS base_tag,
        CASE
            WHEN REGEXP_LIKE(tag_raw, '^voc_') THEN 1
            ELSE 0
        END AS voc_flag
    FROM tag_rows
),

voc_dict_auto AS (
    SELECT DISTINCT base_tag
    FROM normalized
    WHERE voc_flag = 1
),

ticket_voc_candidates AS (
    SELECT
        n.ticket_id,
        n.dt,
        n.base_tag
    FROM normalized n
    JOIN voc_dict_auto d
      ON n.base_tag = d.base_tag
),

ticket_voc_tag AS (
    SELECT
        ticket_id,
        MIN(dt) AS dt,
        MIN(base_tag) AS review
    FROM ticket_voc_candidates
    GROUP BY 1
),

-- =========================
-- 🔥 ДОБАВИЛИ: подтягиваем description
-- =========================
zendesk_enriched AS (
    SELECT
        t.ticket_id,
        t.dt,
        t.review,
        z.description
    FROM ticket_voc_tag t
    LEFT JOIN data_bronze_zendesk_prod.zendesk_tickets z
        ON t.ticket_id = z.ticket_id
),

zendesk_source AS (
    SELECT
        dt AS date,
        'zendesk' AS source,
        description AS text,
        review,
        CAST(ticket_id AS VARCHAR) AS source_id
    FROM zendesk_enriched
),

-- =========================
-- 2) AppFollow
-- =========================
appfollow_source AS (
    SELECT
        CAST(review_published_datetime AS DATE) AS date,
        store AS source,
        content AS text,
        LOWER(TRIM(voc_category)) AS review,
        CAST(review_id AS VARCHAR) AS source_id
    FROM data_silver_appfollow_prod.appfollow_reviews
    WHERE voc_category IS NOT NULL
),

-- =========================
-- 3) Trustpilot
-- =========================
trustpilot_source AS (
    SELECT
        CAST(created_at AS DATE) AS date,
        'trustpilot' AS source,
        text,
        LOWER(TRIM(voc_category)) AS review,
        CAST(review_id AS VARCHAR) AS source_id
    FROM data_silver_trustpilot_prod.trustpilot_reviews
    WHERE voc_category IS NOT NULL
),

-- =========================
-- 4) Объединяем все источники
-- =========================
all_sources AS (
    SELECT * FROM zendesk_source
    UNION ALL
    SELECT * FROM appfollow_source
    UNION ALL
    SELECT * FROM trustpilot_source
),

-- =========================
-- 5) Разбивка review на bucket / leaf
-- =========================
base AS (
    SELECT
        date,
        source,
        text,
        review,
        source_id,
        SPLIT_PART(review, '-', 1) AS bucket,
        SPLIT_PART(review, '-', 2) AS leaf
    FROM all_sources
)

SELECT
    date,
    source,
    text,
    review,
    source_id,
    bucket,
    leaf,
    CASE
        WHEN leaf = 'general_login_issue' THEN 'Technical Bugs & App Stability'
        WHEN leaf = 'mistyped_email' THEN 'Technical Bugs & App Stability'
        WHEN leaf = 'magic_link_issue' THEN 'Technical Bugs & App Stability'
        WHEN leaf = 'email_not_received' THEN 'Limited Access & Locked Content'
        WHEN leaf = 'bug' THEN 'Technical Bugs & App Stability'
        WHEN leaf = 'audio_issues' THEN 'Audio & Voice Issues'
        WHEN leaf = 'video_issues' THEN 'Audio & Voice Issues'
        WHEN leaf = 'green_screen_freeze' THEN 'Technical Bugs & App Stability'
        WHEN leaf = 'daily_streaks' THEN 'Technical Bugs & App Stability'
        WHEN leaf = 'broken_content' THEN 'Technical Bugs & App Stability'
        WHEN leaf = 'smth_went_wrong_screen' THEN 'Technical Bugs & App Stability'
        WHEN leaf = 'font_size' THEN 'Technical Bugs & App Stability'
        WHEN leaf = 'paid_but_no_access' THEN 'Limited Access & Locked Content'
        WHEN leaf = 'unaware_of_subscription' THEN 'Pricing, Billing & Subscription Issues'
        WHEN leaf = 'upset_with_autorenewal' THEN 'Pricing, Billing & Subscription Issues'
        WHEN leaf = 'unauthorized_claims' THEN 'Pricing, Billing & Subscription Issues'
        WHEN leaf = 'unexpected_charges' THEN 'Aggressive Upselling & Pop-Ups'
        WHEN leaf = 'unexpected_charges_upsells' THEN 'Aggressive Upselling & Pop-Ups'
        WHEN leaf = 'expected_a_trial' THEN 'Misleading Advertising & Expectation Mismatch'
        WHEN leaf = 'false_ad' THEN 'Misleading Advertising & Expectation Mismatch'
        WHEN leaf = 'no_longer_interested' THEN 'Lack of Practice & Interactivity'
        WHEN leaf = 'content_too_easy' THEN 'Lack of Practice & Interactivity'
        WHEN leaf = 'too_boring' THEN 'Too Much Reading'
        WHEN leaf = 'content_dissatisfaction' THEN 'Too Much Reading'
        WHEN leaf = 'not_interactive' THEN 'Lack of Practice & Interactivity'
        WHEN leaf = 'legal_threat' THEN 'Pricing, Billing & Subscription Issues'
        WHEN leaf = 'bank_threat' THEN 'Pricing, Billing & Subscription Issues'
        WHEN leaf = 'money_back_guarantee' THEN 'Pricing, Billing & Subscription Issues'
        WHEN leaf = 'feature_request' THEN 'Audio & Voice Issues'
        WHEN leaf = 'general_dissatisfaction' THEN 'Personalization Gaps'
        WHEN leaf = 'ai_dissatisfaction' THEN 'AI Distrust'
        WHEN leaf = 'missing_content' THEN 'Other'
        WHEN leaf = 'no_reason' THEN 'Other'
        WHEN leaf = 'no_details' THEN 'Other'
        WHEN leaf = 'other' THEN 'Other'
        ELSE 'Other'
    END AS product_tag
FROM base
ORDER BY date, source, source_id
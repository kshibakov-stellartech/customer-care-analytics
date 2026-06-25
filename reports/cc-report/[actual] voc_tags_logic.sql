WITH
excluded_tag_patterns AS (
    SELECT *
    FROM (
        VALUES
            ('%cancellation_notification%'),
            ('%closed_by_merge%'),
            ('%voice_abandoned_in_voicemail%'),
            ('%appfollow%'),
            ('%spam%'),
            ('%ai_cb_triggered%'),
            ('%chargeback_precom%'),
            ('%chargeback_postcom%')
    ) AS t(pattern)
),

tickets_to_exclude AS (
    SELECT
        za.ticket_id AS ticket_to_exclude_id,
        MIN(CAST(za.created_at AS DATE)) AS created_date
    FROM data_bronze_zendesk_prod.zendesk_audit za
    JOIN excluded_tag_patterns etp
      ON za.events__field_name = 'tags'
     AND za.events__value LIKE etp.pattern
    WHERE za.created_at >= DATE '2025-11-01'
    GROUP BY 1
),

tickets AS (
    SELECT
        za.ticket_id,
        MIN(za.created_at) AS ticket_created_at,
        CAST(MAX(za.events__value) AS BIGINT) AS requester_id
    FROM data_bronze_zendesk_prod.zendesk_audit za
    WHERE za.events__type = 'Create'
      AND za.events__field_name = 'requester_id'
      AND NOT EXISTS (
          SELECT 1
          FROM tickets_to_exclude te
          WHERE te.ticket_to_exclude_id = za.ticket_id
      )
    GROUP BY 1
    HAVING MIN(CAST(za.created_at AS DATE)) >= DATE '2025-11-01'
       AND MIN(CAST(za.created_at AS DATE)) < current_date
),

users AS (
    SELECT za.ticket_id,
           MAX(
            CASE
                WHEN za.events__field_name IN ('32351109113361', '40831328206865', '32351085497873')
                THEN za.events__value
            END) AS user_id,
            MAX(
            CASE
                WHEN events__type = 'Create' AND events__field_name = 'brand_id' THEN
                    CASE
                        WHEN events__value = '26467992035601' THEN 'MindScape'
                        WHEN events__value = '27810244289553' THEN 'Neurolift'
                        WHEN events__value = '26468032413713' THEN 'SmartyMe'
                        WHEN events__value = '26222456156689' THEN 'StellarTech Limited'
                        WHEN events__value = '43023476289553' THEN 'Nexera'
                        ELSE 'Unknown'
                    END
            END) AS ticket_brand
    FROM data_bronze_zendesk_prod.zendesk_audit za
        JOIN tickets t ON za.ticket_id = t.ticket_id
    GROUP BY 1
),

-- =========================
-- 1) Zendesk: ONLY tickets source
-- audit используем только для excluded
-- =========================
zendesk_source AS (
    SELECT
        CAST(z.created_at AS DATE) AS date,
        'zendesk' AS source,
        z.description AS text,

        CASE
            WHEN z.voc_category IS NULL
              OR TRIM(z.voc_category) = ''
                THEN NULL
            ELSE REGEXP_REPLACE(LOWER(TRIM(z.voc_category)), '^voc_', '')
        END AS review,

        CAST(z.ticket_id AS VARCHAR) AS source_id,

        -- user_id раньше доставался из audit custom fields.
        -- Сейчас Zendesk source = tickets only, поэтому ставим NULL.
        u.user_id AS user_id,
        ticket_brand as brand
    FROM data_bronze_zendesk_prod.zendesk_tickets z
        JOIN tickets t ON z.ticket_id = t.ticket_id
        LEFT JOIN users u ON t.ticket_id = u.ticket_id
    WHERE 1=1
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
        CAST(review_id AS VARCHAR) AS source_id,
        null AS user_id,
        null AS brand
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
        CAST(review_id AS VARCHAR) AS source_id,
        null AS user_id,
        null AS brand
    FROM data_silver_trustpilot_prod.trustpilot_reviews
    WHERE voc_category IS NOT NULL
),

-- =========================
-- 4) Typeforms
-- =========================
    typeform_src AS (
    SELECT
        record_id,
        event_id,
        'typeform_1day' AS source_name,
        json_extract_scalar(json_parse(hidden), '$.user_id') AS user_id,
        CAST(_kinesis__ts AS date) AS dt,
        voc_category,
        CAST(json_parse(definition) AS json) AS definition_json,
        CAST(json_parse(answers) AS array(json)) AS answers_json
    FROM firehose_typeform_1day_webhook_prod.typeform_1day
    WHERE voc_category IS NOT NULL
    UNION ALL
    SELECT
        record_id,
        event_id,
        'typeform_3day' AS source_name,
        json_extract_scalar(json_parse(hidden), '$.user_id') AS user_id,
        CAST(_kinesis__ts AS date) AS dt,
        voc_category,
        CAST(json_parse(definition) AS json) AS definition_json,
        CAST(json_parse(answers) AS array(json)) AS answers_json
    FROM firehose_typeform_3dayplus_webhook_prod.typeform_3dayplus
    WHERE voc_category IS NOT NULL
),

fields_map AS (
    SELECT
        s.record_id,
        s.event_id,
        s.user_id,
        s.source_name,
        s.dt,
        s.voc_category,
        json_extract_scalar(s.definition_json, '$.title') AS ticket_subject,
        CAST(
            map_agg(
                json_extract_scalar(f, '$.id'),
                json_extract_scalar(f, '$.title')
            ) AS map(varchar, varchar)
        ) AS question_map,
        s.answers_json
    FROM typeform_src s
    CROSS JOIN UNNEST(
        CAST(json_extract(s.definition_json, '$.fields') AS array(json))
    ) AS t(f)
    GROUP BY
        s.record_id,
        s.event_id,
        s.user_id,
        s.source_name,
        s.dt,
        s.voc_category,
        json_extract_scalar(s.definition_json, '$.title'),
        s.answers_json
),

answers_expanded AS (
    SELECT
        fm.record_id,
        fm.event_id,
        fm.user_id,
        fm.source_name,
        fm.dt,
        fm.voc_category,
        fm.ticket_subject,
        ord AS answer_pos,

        element_at(
            fm.question_map,
            json_extract_scalar(a, '$.field.id')
        ) AS question,

        json_extract_scalar(a, '$.type') AS answer_type,
        a AS answer_json
    FROM fields_map fm
    CROSS JOIN UNNEST(fm.answers_json) WITH ORDINALITY AS t(a, ord)
),

voc_filtered AS (
    SELECT
        record_id,
        event_id,
        user_id,
        source_name,
        dt,
        voc_category,
        ticket_subject,
        answer_pos,
        question,

        CASE
            WHEN answer_type = 'text'
                THEN json_extract_scalar(answer_json, '$.text')

            WHEN answer_type = 'choice'
                THEN json_extract_scalar(answer_json, '$.choice.label')

            WHEN answer_type = 'choices'
                THEN array_join(
                    CAST(json_extract(answer_json, '$.choices.labels') AS array(varchar)),
                    ', '
                )

            WHEN answer_type = 'number'
                THEN json_extract_scalar(answer_json, '$.number')

            WHEN answer_type = 'boolean'
                THEN CASE
                    WHEN json_extract_scalar(answer_json, '$.boolean') = 'true' THEN 'Yes'
                    ELSE 'No'
                END

            WHEN answer_type = 'email'
                THEN json_extract_scalar(answer_json, '$.email')

            WHEN answer_type = 'url'
                THEN json_extract_scalar(answer_json, '$.url')

            WHEN answer_type = 'date'
                THEN json_extract_scalar(answer_json, '$.date')

            ELSE json_format(answer_json)
        END AS answer_value

    FROM answers_expanded
    WHERE question IS NOT NULL
      AND (
            lower(question) LIKE '%how likely are you to recommend%'
         OR lower(question) LIKE '%make smartyme more useful%'
         OR lower(question) LIKE '%what do you like most about the app%'
         OR lower(question) LIKE '%change or add to improve your experience%'
         OR lower(question) LIKE '%features or improvements would you like to see%'
         OR lower(question) LIKE '%sharing a brief review about your overall experience%'
         OR lower(question) LIKE '%first impression about smartyme%'
         OR lower(question) LIKE '%elaborate on your impression%'
         OR lower(question) LIKE '%frustrated or confused you%'
         OR lower(question) LIKE '%feature or improvement you wish smartyme had%'
      )
),

final_msg AS (
    SELECT
        record_id,
        event_id,
        user_id,
        source_name,
        dt,
        voc_category,

        max(ticket_subject) AS ticket_subject,

        array_join(
            array_agg(
                format('Q: %s\nA: %s', question, answer_value)
                ORDER BY answer_pos
            ),
            '\n\n'
        ) AS ticket_comment

    FROM voc_filtered
    WHERE answer_value IS NOT NULL
      AND answer_value <> ''
    GROUP BY
        record_id,
        event_id,
        user_id,
        source_name,
        dt,
        voc_category
),
    typeform_source AS (
SELECT dt AS date,
       source_name AS source,
       ticket_comment as text,
       LOWER(TRIM(voc_category)) AS review,
       event_id AS source_id,
       user_id,
       null AS brand
FROM final_msg
),

-- =========================
-- 5) Объединяем все источники
-- =========================
all_sources AS (
    SELECT * FROM zendesk_source
    UNION ALL
    SELECT * FROM appfollow_source
    UNION ALL
    SELECT * FROM trustpilot_source
    UNION ALL
    SELECT * FROM typeform_source
),

-- =========================
-- 6) Разбивка review на bucket / leaf
-- =========================
base AS (
    SELECT
        date,
        source,
        text,
        review,
        source_id,
        user_id,
        brand,
        SPLIT_PART(review, '-', 1) AS bucket,
        SPLIT_PART(review, '-', 2) AS leaf
    FROM all_sources
),
    tags_categorized_complete AS (
SELECT
    date,
    source,
    text,
    review,
    source_id,
    user_id,
    brand,
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
),
    user_meta AS (
select
    profile_id as user_id,
    any_value(traffic_source) as traffic_source,
    any_value(placement) as placement,
    any_value(payment_type) as payment_type,
    any_value(wallet_type) as wallet_type,
    any_value(user_gender) as user_gender,
    any_value(user_age) as user_age,
    any_value(country) as country,
    max(
        case
            when vendor_subscription_id is not null
             and parent_subscription_id is not null
             and vendor_subscription_id = parent_subscription_id then 'main'
            when vendor_subscription_id is not null
             and parent_subscription_id is not null
             and vendor_subscription_id <> parent_subscription_id then 'main+upcell'
            else null
        end
    ) as subscription_type
FROM data_silver_product_sessions_prod.sf_purchase_sessions
WHERE 1=1
  AND subscription_created_at >= DATE '2025-01-01'
group by 1
)

SELECT tcc.*,
       traffic_source,
       placement,
       payment_type,
       wallet_type,
       user_gender,
       user_age,
       country,
       subscription_type
FROM tags_categorized_complete tcc
    LEFT JOIN user_meta um ON tcc.user_id = um.user_id
WHERE 1=1
  AND date >= DATE '2025-11-01'
  AND source = 'zendesk'
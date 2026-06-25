WITH
/* =========================================================
0. reference dictionaries
========================================================= */
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

auto_reply_titles AS (
    SELECT *
    FROM (
        VALUES
            ('Auto_12: Auto-reply to refund requests (Stores)'),
            ('Auto_21: Auto-reply to delete+refund requests (Paddle/PayPal)'),
            ('Auto_91: Auto-reply to delete requests (Stores)'),
            ('Auto_13: Auto-reply to refund requests (Paddle/PayPal)'),
            ('Auto_29: Auto-reply - payment_not_found AI'),
            ('Auto_29: Auto-reply - payment_not_found AI (2nd)'),
            ('Auto_29: Auto-reply - payment_not_found (automation failed)'),
            ('Auto_35: Auto-reply to delete+refund requests (threats/risk)'),
            ('Auto_6: Auto-reply to cancel requests (Web) '),
            ('Auto_7: Auto-reply to cancel requests (Stores)'),
            ('Auto_28: Freemium only - payment_not_found'),
            ('Auto-reply - something is wrong with my subscription - SmartyMe')
    ) AS t(from_title)
),

agents_dict AS (
    SELECT
        za.agent_id,
        za.name AS agent_name,
        CASE
            WHEN zg.group_id IN (39601781732369, 39601932203409) THEN zg.name
            WHEN za.agent_id = 26440502459665 THEN 'Automation'
            ELSE 'Admins'
        END AS agent_group
    FROM data_bronze_zendesk_prod.zendesk_agents za
    LEFT JOIN data_bronze_zendesk_prod.zendesk_group_memberships zgm
        ON zgm.user_id = za.agent_id
       AND zgm.group_id IN (39601781732369, 39601932203409)
    LEFT JOIN data_bronze_zendesk_prod.zendesk_groups zg
        ON zg.group_id = zgm.group_id
),

/* =========================================================
1. tickets period + exclusions
========================================================= */
tickets_to_exclude AS (
    SELECT DISTINCT
        za.ticket_id
    FROM data_bronze_zendesk_prod.zendesk_audit za
    JOIN excluded_tag_patterns etp
        ON za.events__field_name = 'tags'
       AND za.events__value LIKE etp.pattern
    WHERE za.created_at >= DATE '2026-05-26'
      AND za.created_at <  DATE '2026-06-17'
),

tickets AS (
    SELECT
        za.ticket_id,
        MIN(date_add('hour', 2, za.created_at)) AS ticket_created_at
    FROM data_bronze_zendesk_prod.zendesk_audit za
    WHERE za.events__type = 'Create'
      AND za.events__field_name = 'requester_id'
    GROUP BY 1
    HAVING CAST(MIN(date_add('hour', 2, za.created_at)) AS DATE) >= DATE '2026-05-26'
       AND CAST(MIN(date_add('hour', 2, za.created_at)) AS DATE) <= DATE '2026-06-16'
),

base_audit AS (
    SELECT
        za.ticket_id,
        t.ticket_created_at,
        date_add('hour', 2, za.created_at) AS created_at,
        CAST(CAST(za.author_id AS DOUBLE) AS BIGINT) AS author_id,
        CAST(CAST(za.events__author_id AS DOUBLE) AS BIGINT) AS event_author_id,
        za.events__id,
        za.events__type,
        za.events__field_name,
        za.events__value,
        za.events__public,
        za.events__from_title
    FROM data_bronze_zendesk_prod.zendesk_audit za
    JOIN tickets t
        ON t.ticket_id = za.ticket_id
    LEFT JOIN tickets_to_exclude te
        ON te.ticket_id = za.ticket_id
    WHERE te.ticket_id IS NULL
),

/* =========================================================
2. first agent/public automation message per ticket
========================================================= */
agent_messages AS (
    SELECT
        b.ticket_id,
        b.ticket_created_at,
        b.created_at,
        b.events__id AS event_id,
        CASE
            WHEN b.events__type = 'Notification' THEN 26440502459665
            ELSE b.author_id
        END AS agent_id
    FROM base_audit b
    LEFT JOIN agents_dict ad
        ON ad.agent_id = b.author_id
    WHERE
        (
            b.events__type = 'Comment'
            AND b.events__public = TRUE
            AND ad.agent_id IS NOT NULL
        )
        OR
        (
            b.events__type = 'Notification'
            AND EXISTS (
                SELECT 1
                FROM auto_reply_titles art
                WHERE art.from_title = b.events__from_title
            )
        )
),

first_agent_message AS (
    SELECT *
    FROM (
        SELECT
            am.*,
            ROW_NUMBER() OVER (
                PARTITION BY am.ticket_id
                ORDER BY am.created_at, am.event_id
            ) AS rn
        FROM agent_messages am
    )
    WHERE rn = 1
),

/* =========================================================
3. directive flags
========================================================= */
directive_flags AS (
    SELECT
        ticket_id,
        MAX(
            CASE
                WHEN events__field_name = 'tags'
                 AND events__value LIKE '%directive_adopted%'
                THEN 1 ELSE 0
            END
        ) AS directive_adopted,
        MAX(
            CASE
                WHEN events__field_name = 'tags'
                 AND events__value LIKE '%directive_neglected%'
                THEN 1 ELSE 0
            END
        ) AS directive_neglected
    FROM base_audit
    GROUP BY 1
),

/* =========================================================
4. VOC bucket / leaf
========================================================= */
tag_rows AS (
    SELECT
        za.ticket_id,
        CAST(za.created_at AS DATE) AS dt,
        LOWER(TRIM(tag)) AS tag_raw
    FROM data_bronze_zendesk_prod.zendesk_audit za
    JOIN tickets t
        ON t.ticket_id = za.ticket_id
    CROSS JOIN UNNEST(SPLIT(za.events__value, ',')) AS u(tag)
    WHERE za.events__field_name = 'tags'
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
    SELECT DISTINCT
        base_tag
    FROM normalized
    WHERE voc_flag = 1
),

ticket_voc_tag AS (
    SELECT
        n.ticket_id,
        MIN(n.dt) AS dt,
        MIN(n.base_tag) AS review
    FROM normalized n
    JOIN voc_dict_auto d
        ON d.base_tag = n.base_tag
    GROUP BY 1
),

voc_bucket_leaf AS (
    SELECT
        ticket_id,
        SPLIT_PART(review, '-', 1) AS bucket,
        SPLIT_PART(review, '-', 2) AS leaf
    FROM ticket_voc_tag
)

SELECT
    fam.ticket_created_at,
    fam.ticket_id,
    CONCAT('https://stellartechlimited.zendesk.com/agent/tickets/', CAST(fam.ticket_id AS VARCHAR)) as ticket_link,
    ad.agent_name,
    ad.agent_group,
    vbl.bucket as voc_main_category,
    vbl.leaf as voc_sub_category,
    COALESCE(df.directive_adopted, 0) AS directive_adopted,
    COALESCE(df.directive_neglected, 0) AS directive_neglected
FROM first_agent_message fam
LEFT JOIN agents_dict ad
    ON ad.agent_id = fam.agent_id
LEFT JOIN voc_bucket_leaf vbl
    ON vbl.ticket_id = fam.ticket_id
LEFT JOIN directive_flags df
    ON df.ticket_id = fam.ticket_id
WHERE fam.agent_id <> 26440502459665
ORDER BY
    fam.ticket_created_at,
    fam.ticket_id;
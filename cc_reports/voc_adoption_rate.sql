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
SELECT za.agent_id,
       za.name as agent_name,
       CASE WHEN zg.group_id IN (39601781732369, 39601932203409) THEN zg.name
            WHEN za.agent_id = 26440502459665 THEN 'Automation'
            ELSE 'Admins'
       END as agent_group
FROM data_bronze_zendesk_prod.zendesk_agents za
    LEFT JOIN data_bronze_zendesk_prod.zendesk_group_memberships zgm ON zgm.user_id = za.agent_id
                                                                    AND zgm.group_id IN (39601781732369, 39601932203409)
    LEFT JOIN data_bronze_zendesk_prod.zendesk_groups zg ON zg.group_id = zgm.group_id
),

tickets_to_exclude AS (
    SELECT
        za.ticket_id AS ticket_to_exclude_id,
        MIN(CAST(za.created_at AS DATE)) AS created_date
    FROM data_bronze_zendesk_prod.zendesk_audit za
    JOIN excluded_tag_patterns etp
      ON za.events__field_name = 'tags'
     AND za.events__value LIKE etp.pattern
    WHERE za.created_at >= DATE '2026-03-01'
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
    GROUP BY 1
    HAVING MIN(CAST(za.created_at AS DATE)) >= DATE '2026-03-01'
       AND MIN(CAST(za.created_at AS DATE)) < current_date
),

base_audit AS (
    SELECT
        za.ticket_id,
        t.ticket_created_at,
        t.requester_id,
        za.channel,
        date_add('hour', 2, za.created_at) AS created_at,
        date_trunc('minute', date_add('hour', 2, za.created_at)) AS created_at_truncated,
        CAST(CAST(za.author_id AS DOUBLE) AS BIGINT) AS author_id,
        CAST(CAST(za.events__author_id AS DOUBLE) AS BIGINT) AS event_author_id,
        za.events__id,
        za.events__type,
        za.events__field_name,
        za.events__value,
        TRY_CAST(za.events__value AS BIGINT) AS events__value_bigint,
        za.events__previous_value,
        za.events__body,
        za.events__public,
        za.events__from_title,
        CASE
            WHEN za.events__type = 'Notification'
             AND EXISTS (
                SELECT 1
                FROM auto_reply_titles art
                WHERE art.from_title = za.events__from_title
             )
            THEN 1
            WHEN CAST(CAST(za.events__author_id AS DOUBLE) AS BIGINT) IS NOT NULL
             AND za.events__public = TRUE
            THEN 2
            ELSE 0
        END AS is_public_communication
    FROM data_bronze_zendesk_prod.zendesk_audit za
    JOIN tickets t
      ON t.ticket_id = za.ticket_id
    LEFT JOIN tickets_to_exclude te
      ON te.ticket_to_exclude_id = za.ticket_id
    WHERE te.ticket_to_exclude_id IS NULL
      -- AND za.ticket_id = 762083
),
    message_events AS (
    SELECT
        b.ticket_id,
        b.created_at,
        b.events__id AS event_id,
        CASE
            WHEN b.events__type = 'Notification' THEN 26440502459665
            ELSE b.author_id
        END AS author_id,
        b.requester_id,
        CASE
            WHEN b.events__type = 'Comment'
             AND b.events__public = TRUE
             AND ad.agent_id IS NULL
            THEN 'customer_message'
            WHEN b.events__type = 'Comment'
             AND b.events__public = TRUE
             AND ad.agent_id IS NOT NULL
            THEN 'agent_message'
            WHEN b.events__type = 'Notification'
             AND b.is_public_communication IN (1, 2)
            THEN 'agent_message'
            ELSE 'unknown'
        END AS event_type,
        b.events__body AS msg_text,
        b.events__from_title,
        ROW_NUMBER() OVER(PARTITION BY b.ticket_id ORDER BY b.created_at) as rn
    FROM base_audit b
      JOIN agents_dict ad
      ON ad.agent_id = b.author_id
    WHERE
        (
            b.events__type = 'Comment'
            AND b.events__public = TRUE
        )
        OR
        (
            b.events__type = 'Notification'
            AND b.is_public_communication IN (1, 2)
        )
),

tickets_attr AS (
    SELECT
        b.ticket_id,
        b.ticket_created_at,
        CAST(CAST(b.requester_id AS DOUBLE) AS BIGINT) AS requester_id,
        me.author_id,
        CASE WHEN me.author_id <> 26440502459665 THEN 1 ELSE 0 END as first_reply_from_person,
        MAX(CASE WHEN events__field_name = 'assignee_id' AND TRY_CAST(events__value AS BIGINT) = 26440502459665 THEN 1  ELSE 0 END) as auto_involved,
        CASE WHEN ELEMENT_AT(
           ARRAY_AGG(b.author_id ORDER BY b.created_at DESC, b.events__id DESC)
           FILTER (WHERE events__type = 'Comment' AND events__public = true), 1
       ) = 26440502459665 THEN 1 ELSE 0 END as auto_resolved,
        MAX(CASE WHEN events__type = 'Comment' AND events__body LIKE 'Suggestion from Nikki:%' THEN 1 ELSE 0 END) as auto_suggest,
        MAX(CASE WHEN events__field_name = 'tags' AND events__value LIKE 'not_found_cs' THEN 1 ELSE 0 END) as not_found_cs
    FROM base_audit b
        LEFT JOIN message_events me ON me.ticket_id = b.ticket_id
                                   AND me.rn = 1
    GROUP BY 1, 2, 3, 4
),


/* =========================================================
VOC bucket / leaf from second script
========================================================= */
tag_rows AS (
    SELECT
        ticket_id,
        CAST(created_at AS DATE) AS dt,
        LOWER(TRIM(tag)) AS tag_raw
    FROM data_bronze_zendesk_prod.zendesk_audit
    CROSS JOIN UNNEST(SPLIT(events__value, ',')) AS u(tag)
    WHERE 1=1
      AND created_at >= DATE '2026-03-01'
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

voc_bucket_leaf AS (
    SELECT
        ticket_id,
        SPLIT_PART(review, '-', 1) AS bucket,
        SPLIT_PART(review, '-', 2) AS leaf
    FROM ticket_voc_tag
),
    res AS (
SELECT ta.*,
       vbl.bucket as voc_main_cat,
       vbl.leaf as voc_sub_cat,
       ROW_NUMBER() OVER (PARTITION BY vbl.bucket, vbl.leaf ORDER BY ta.ticket_id DESC) as rn
FROM tickets_attr ta
    JOIN voc_bucket_leaf vbl ON vbl.ticket_id = ta.ticket_id
WHERE 1=1
  AND auto_suggest = 1
  AND first_reply_from_person = 1
  AND not_found_cs = 0
),
    agg_data AS (
SELECT voc_main_cat,
       voc_sub_cat,
       COUNT(ticket_id) as cnt,
       MAX(rn) as max_rn,
       MIN(rn) as min_rn
FROM res
GROUP BY 1, 2
),
    final AS (
SELECT res.*
FROM res
    JOIN agg_data ad ON ad.voc_sub_cat = res.voc_sub_cat
                    AND ad.voc_main_cat = res.voc_main_cat
                    --AND ad.cnt >= 10
--AND res.rn <= 10
),

    res2 AS (
SELECT DISTINCT voc_main_cat, voc_sub_cat, agent_name, agent_group, final.ticket_id
FROM final
    LEFT JOIN tickets_attr ON final.ticket_id = tickets_attr.ticket_id
    LEFT JOIN agents_dict ON tickets_attr.author_id = agents_dict.agent_id
WHERE 1=1
)

SELECT *
FROM res2
WHERE ticket_id IN (
1101581,
1097984,
1097212,
1094709,
1093105,
1088898,
1088367,
1081753,
1076626,
1070234,
1104434,
1102771,
1101205,
1100281,
1098339,
1098086,
1094499,
1091957,
1091016,
1089337,
1104477,
1104459,
1103418,
1093981,
1090525,
1087252,
1079225,
1075651,
1074863,
1073902,
1104606,
1104230,
1103942,
1103939,
1103938,
1103612,
1103606,
1102823,
1102821,
1102147,
1104779,
1104352,
1103905,
1103891,
1103814,
1103522,
1103519,
1103309,
1103022,
1102746,
1103978,
1103421,
1098173,
1097055,
1088117,
1056835,
1008285,
992324,
982425,
978912,
1098662,
1074169,
1072947,
1072837,
1072532,
1067569,
1064392,
1062787,
1033601,
1022320,
1096984,
1096273,
1089346,
1077347,
1077346,
1077061,
1076041,
1075744,
1074025,
1073834,
1101939,
1097500,
1086574,
1077927,
1077106,
1075314,
1069381,
1067938,
1100528,
1095349,
1092261,
1088618,
1087151,
1074314,
1071093,
1070056,
1068925,
1061839,
1059303,
1058285,
1104098,
1103980,
1101103,
1100602,
1097992,
1096602,
1095016,
1092814,
1091586,
1089962,
1104773,
1102741,
1102608,
1102045,
1101411,
1100403,
1100311,
1099301,
1099011,
1098604,
1104770,
1103754,
1102977,
1102654,
1100873,
1100833,
1100728,
1098028,
1096836,
1095404,
1104717,
1103975,
1103857,
1103804,
1103312,
1103263,
1103253,
1103196,
1103067,
1102850,
1102569,
1102433,
1101092,
1099841,
1099125,
1098100,
1097289,
1096012,
1094495,
1094249,
1104056,
1097220,
1095556,
1095427,
1091946,
1091899,
1086083,
1085777,
1085650,
1083550,
1087030,
1081914,
1073327,
1072431,
1071032,
1070913,
1065358,
1065252,
1061070,
1059099,
1102927,
1087360,
1086438,
1073101,
1068917,
1048221,
1040880,
1040178,
1018520,
1018487,
1091767,
1083994,
1081065,
1079878,
1079841,
1079162,
1078770,
1075241,
1074271,
1071565,
1104183,
1102768,
1098778,
1093499,
1092329,
1091412,
1088657,
1086427,
1079077,
1078427,
1099700,
1097024,
1081120,
1078958,
1077536,
1077103,
1075807,
1074985,
1073325,
1072834,
1096921,
1095299,
1093409,
1090752,
1088946,
1085894,
1080351,
1080180,
1078962,
1078764,
1090995,
1079172,
1079082,
1078994,
1073957,
1073583,
1070319,
1069908,
1067314,
1064594,
1104485,
1104369,
1104179,
1103659,
1103610,
1103547,
1102176,
1102082,
1101904,
1101685,
1076568,
1063577,
1034018,
1004150,
998769,
981534,
981531,
976457,
975561,
975134,
1102702,
1102117,
1099920,
1099155,
1098618,
1095980,
1093015,
1089891,
1089632,
1087996,
1101936,
1101905,
1100969,
1100321,
1099297,
1097749,
1095991,
1095136,
1094604,
1093990,
1103799,
1103757,
1103654,
1103150,
1103112,
1102986,
1101005,
1099567,
1097711,
1094278,
1102081,
1099666,
1096210,
1088402,
1086528,
1086075,
1085200,
1083167,
1080702,
1079595,
1103034,
1078762,
1077152,
1076830,
1075718,
1072449,
1071912,
1071689,
1071613,
1071591,
1076784,
1064548,
1059299,
1053352,
1053035,
1052520,
1051341,
1047130,
1042604,
1038717,
1104555,
1103936,
1103411,
1103410,
1103363,
1102948,
1102879,
1102851,
1102849,
1102633,
1102587,
1099970,
1099366,
1095344,
1087355,
1082430,
1079932,
1078731,
1078677,
1077763,
1102609,
1099836,
1099672,
1098721,
1098088,
1095233,
1094550,
1094033,
1093794,
1091891,
1104436,
1103355,
1101496,
1100766,
1098666,
1098103,
1097156,
1097147,
1096689,
1096205,
1103611,
1094707,
1082426,
1078757,
1077235,
1074710,
1073577,
1072740,
1068351,
1042843,
1102567,
1101652,
1100392,
1093421,
1088065,
1083030,
1076679,
1065398,
1065204,
1065002,
1077644,
1074612,
1072310,
1070990,
1070138,
1069264,
1067502,
1067200,
1066275,
1065504,
1104716,
1104482,
1103898,
1103665,
1103663,
1103308,
1102855,
1102845,
1102792,
1102539,
1104720,
1104019,
1102770,
1102083,
1101514,
1100729,
1100498,
1099867,
1099094,
1096862,
1094922,
1092987,
1088463,
1086129,
1047816,
1047691,
1040319,
999150,
990438,
982549,
1060375,
1056188,
1106718,
1107346,
1106914,
1106297,
1105098,
1102071,
1101409,
1100402,
1095557,
1094312,
1090350,
1074676,
996785,
993021,
1107572,
1093186,
1087763,
1086782,
1086225,
1085822,
1080852,
1075689,
1106716,
1102380,
1100238,
1098491,
1098135,
1097293,
1096981,
1095086,
1106061,
1105173,
1104832,
1105335,
1073771,
1068630,
1064114,
1051307,
1050679,
1042607,
1037849,
1027027,
995714,
984821,
971471,
963739,
962478,
1106263,
1106086,
1106032,
1095892,
1092202,
1091702,
1091353,
1090956,
1088906,
1088304,
1065143,
1077583,
1077010,
1076965,
1076871,
1076720,
1106577,
1107004,
1105440,
1105360,
1105959,
1089173,
1086506,
1077207,
1076974,
1063635,
985174,
969388,
968128,
957202,
1107249,
1106662,
1106074,
1106039,
1105679,
1105460,
1105181,
1022235,
1107512,
1106483,
1105457,
1107256,
1101457,
1089393,
1088062,
1084914,
1084145,
1082732,
1082492,
1080691,
1070083,
1072844,
1106812,
1107703,
1107573,
1106070,
1102494,
1102012,
1101508,
1100358,
1099892,
1095473,
1107333,
1107253,
1106814,
1106437,
1106311,
1106227,
1105895,
1105591,
1104951,
1104948,
1097008,
1089249,
1076520,
1075715,
1071031,
1068035,
1064320,
1063279,
1061395,
1107671,
1106215,
1106030,
1105179,
1104909,
1107007,
1093012,
1092580,
1090683,
1090102,
1073105,
1072998,
1107088,
1105717,
1106909,
1106087,
1105237,
1082819,
1075389,
1072931,
1071092,
1055223,
1050419,
1047325,
1047221,
1046554,
1071513,
1067717,
1051756,
1032707,
1030351,
1012719,
1105847,
1100938,
1100932,
1100661,
1099925,
1102036,
1099403,
1099199,
1096110,
1096013,
1093557,
1076624,
1076476,
1063353,
1057621,
1054688,
1039818,
1034990,
1031008,
1021103,
983934,
1106705,
1106622,
1106235,
1106941,
1101491,
1099753,
1097179,
1095891,
1083853,
1082771,
1071632,
1070977,
1070180,
1100803,
1100013,
1098811,
1084506,
1080520,
1071135,
1066787,
1057502,
1106420,
1105898,
1102406,
1101770,
1097821,
1097299,
1096865,
1096572,
1095140,
1093894,
1095590,
1091306,
1081720,
1081485,
1077378,
1076039,
1075809,
1106055,
1105390,
1091883,
1087676,
1075026,
1071867,
1070765,
1051215,
1106949,
1105714,
1105545,
1105455,
1105027,
1104840,
1107674,
1105963,
1104831,
1088246,
1085482,
1079531,
1074258,
1073963,
1067505,
1036166,
1031793,
1018887,
1092271,
1089293,
1085319,
1079972,
1079314
    )
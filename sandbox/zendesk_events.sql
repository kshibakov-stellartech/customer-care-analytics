WITH raw_events AS (
    SELECT ticket_id,
           MAX(CASE WHEN event_type = 'reply_by_agent' AND event_author_id = 26440502459665 THEN 1 ELSE 0 END) as automation_flag,
           MAX(CASE WHEN event_type = 'SurveyOffered' THEN 1 ELSE 0 END) as survey_offered,
           MAX(CASE WHEN event_type = 'SurveyResponseSubmitted' THEN 1 ELSE 0 END) as survey_submitted,
           MAX(CASE WHEN event_type = 'satisfaction_score_change' AND old_value <> 'unoffered' THEN 1 ELSE 0 END) as survey_score_change,
           MAX(CASE WHEN event_type = 'satisfaction_comment_change' THEN 1 ELSE 0 END) as survey_comment_change
    FROM data_silver_zendesk_prod.zendesk_events
    WHERE 1=1
      AND ticket_created_at >= DATE '2025-11-01'
      AND ticket_created_at <  DATE '2025-12-01'
      --AND ticket_id = 593604
      --AND (lower(event_type) LIKE '%chat%' OR lower(event_type) LIKE '%status%' OR lower(event_type) LIKE '%reply%')
      --AND event_type = 'status_change'
    GROUP BY 1
    --ORDER BY event_at
)

SELECT automation_flag,
       COUNT(ticket_id) as tikets,
       SUM(survey_offered) as survey_offered,
       SUM(survey_submitted) as survey_submitted,
       SUM(survey_score_change) as survey_score_change,
       SUM(survey_comment_change) as survey_comment_change
FROM raw_events
GROUP BY 1
ORDER BY 1, 2

;

SELECT *
FROM data_silver_zendesk_prod.zendesk_events
WHERE 1=1
  --AND ticket_created_at >= DATE '2025-11-01'
  --AND ticket_created_at <  DATE '2025-12-01'
  AND ticket_id = 584787
  --AND (lower(event_type) LIKE '%chat%' OR lower(event_type) LIKE '%status%' OR lower(event_type) LIKE '%reply%')
  --AND event_type = 'status_change'
--GROUP BY 1
ORDER BY event_at
;

with ticket_found_tags AS (
SELECT ticket_id,
       CASE WHEN event_type = 'create_requester_id' THEN new_value END as requester_id,
       MAX(CASE WHEN event_type = 'ticket_tag_change' AND new_value LIKE '% found_bo%'    THEN 1 ELSE 0 END) as found_bo,
       MAX(CASE WHEN event_type = 'ticket_tag_change' AND new_value LIKE '% found_cs%'    THEN 1 ELSE 0 END) as found_cs,
       MAX(CASE WHEN event_type = 'ticket_tag_change' AND new_value LIKE '% found_sb%'    THEN 1 ELSE 0 END) as found_sb,
       MAX(CASE WHEN event_type = 'ticket_tag_change' AND new_value LIKE '% found_check%' THEN 1 ELSE 0 END) as found_check
FROM data_silver_zendesk_prod.zendesk_events
WHERE 1=1
  AND ticket_created_at >= DATE '2025-11-01'
  AND ticket_created_at <  DATE '2025-12-01'
GROUP BY 1, 2
  --AND ticket_id = 593604
  --AND (lower(event_type) LIKE '%chat%' OR lower(event_type) LIKE '%status%' OR lower(event_type) LIKE '%reply%')
  --AND event_type = 'status_change'
)

SELECT COUNT(DISTINCT requester_id) as users_cnt,
       SUM(found_bo) as found_bo,
       SUM(found_cs) as found_cs,
       SUM(found_sb) as found_sb,
       SUM(found_check) as found_check
FROM ticket_found_tags
;

SELECT ticket_form_type,
       ticket_type,
       COUNT(DISTINCT ticket_id) as ticket_cnt
FROM ticket_form_type
WHERE ticket_form_type is not null
GROUP BY 1, 2

/*
26472204214801 - main ticket form
34833592831505 - in-app ticket form
34902185196177 - test form
26222488220945 - default ticket form
35743604923281 - registration form
*/
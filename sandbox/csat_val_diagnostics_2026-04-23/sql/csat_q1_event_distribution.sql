SELECT
  events__type,
  events__field_name,
  events__value,
  COUNT(*) AS cnt
FROM data_bronze_zendesk_prod.zendesk_audit
WHERE created_at >= DATE '2026-01-01'
  AND (
    events__field_name = 'satisfaction_score'
    OR events__type IN ('SurveyOffered','SurveyResponseSubmitted')
  )
GROUP BY 1,2,3
ORDER BY cnt DESC
LIMIT 50

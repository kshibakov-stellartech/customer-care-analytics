WITH disputes AS (
  SELECT * FROM (VALUES
    'du_1SDdu0HFFv8NmmxdBfbAuUjf',
    'du_1SDfuoHFFv8NmmxdOGVwOO9F'
  ) AS t(dispute_id)
), unioned AS (
  SELECT 'created' AS source_table, id AS event_id, type AS event_type, created AS event_created_ts,
         data__object__id AS dispute_id, data__object__created AS dispute_created_ts,
         data__object__reason AS reason, data__object__status AS status,
         data__object__amount AS amount_minor, data__object__currency AS currency,
         data__object__evidence_details__has_evidence AS has_evidence,
         data__object__evidence_details__due_by AS due_by_ts,
         data__object__evidence_details__submission_count AS submission_count,
         CAST(NULL AS varchar) AS prev_status,
         _ingested_at
  FROM data_bronze_stripe_prod.stripe_charge_dispute_created
  WHERE data__object__id IN (SELECT dispute_id FROM disputes)

  UNION ALL

  SELECT 'updated' AS source_table, id AS event_id, type AS event_type, created AS event_created_ts,
         data__object__id AS dispute_id, data__object__created AS dispute_created_ts,
         data__object__reason AS reason, data__object__status AS status,
         data__object__amount AS amount_minor, data__object__currency AS currency,
         data__object__evidence_details__has_evidence AS has_evidence,
         data__object__evidence_details__due_by AS due_by_ts,
         data__object__evidence_details__submission_count AS submission_count,
         data__previous_attributes__status AS prev_status,
         _ingested_at
  FROM data_bronze_stripe_prod.stripe_charge_dispute_updated
  WHERE data__object__id IN (SELECT dispute_id FROM disputes)

  UNION ALL

  SELECT 'closed' AS source_table, id AS event_id, type AS event_type, created AS event_created_ts,
         data__object__id AS dispute_id, data__object__created AS dispute_created_ts,
         data__object__reason AS reason, data__object__status AS status,
         data__object__amount AS amount_minor, data__object__currency AS currency,
         data__object__evidence_details__has_evidence AS has_evidence,
         data__object__evidence_details__due_by AS due_by_ts,
         data__object__evidence_details__submission_count AS submission_count,
         data__previous_attributes__status AS prev_status,
         _ingested_at
  FROM data_bronze_stripe_prod.stripe_charge_dispute_closed
  WHERE data__object__id IN (SELECT dispute_id FROM disputes)

  UNION ALL

  SELECT 'funds_withdrawn' AS source_table, id AS event_id, type AS event_type, created AS event_created_ts,
         data__object__id AS dispute_id, data__object__created AS dispute_created_ts,
         data__object__reason AS reason, data__object__status AS status,
         data__object__amount AS amount_minor, data__object__currency AS currency,
         data__object__evidence_details__has_evidence AS has_evidence,
         data__object__evidence_details__due_by AS due_by_ts,
         data__object__evidence_details__submission_count AS submission_count,
         CAST(NULL AS varchar) AS prev_status,
         _ingested_at
  FROM data_bronze_stripe_prod.stripe_charge_dispute_funds_withdrawn
  WHERE data__object__id IN (SELECT dispute_id FROM disputes)
)
SELECT dispute_id,
       source_table,
       event_type,
       event_id,
       from_unixtime(event_created_ts) AS event_created_utc,
       from_unixtime(dispute_created_ts) AS dispute_created_utc,
       reason,
       status,
       amount_minor,
       amount_minor / 100.0 AS amount_major,
       upper(currency) AS currency,
       has_evidence,
       submission_count,
       from_unixtime(due_by_ts) AS due_by_utc,
       prev_status,
       _ingested_at
FROM unioned
ORDER BY dispute_id, event_created_ts, _ingested_at;

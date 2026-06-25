WITH disputes AS (
  SELECT * FROM (VALUES
    'du_1SDdu0HFFv8NmmxdBfbAuUjf',
    'du_1SDfuoHFFv8NmmxdOGVwOO9F'
  ) AS t(dispute_id)
), unioned AS (
  SELECT 'created' AS source_table, id AS event_id, type AS event_type, created AS event_created_ts,
         data__object__id AS dispute_id, data__object__created AS dispute_created_ts,
         data__object__reason AS reason, data__object__status AS status,
         data__object__amount AS amount_minor, upper(data__object__currency) AS currency,
         data__object__evidence_details__has_evidence AS has_evidence,
         data__object__evidence_details__submission_count AS submission_count,
         data__object__evidence_details__due_by AS due_by_ts,
         CAST(NULL AS varchar) AS prev_status,
         _ingested_at
  FROM data_bronze_stripe_prod.stripe_charge_dispute_created
  WHERE data__object__id IN (SELECT dispute_id FROM disputes)
  UNION ALL
  SELECT 'updated', id, type, created,
         data__object__id, data__object__created,
         data__object__reason, data__object__status,
         data__object__amount, upper(data__object__currency),
         data__object__evidence_details__has_evidence,
         data__object__evidence_details__submission_count,
         data__object__evidence_details__due_by,
         data__previous_attributes__status,
         _ingested_at
  FROM data_bronze_stripe_prod.stripe_charge_dispute_updated
  WHERE data__object__id IN (SELECT dispute_id FROM disputes)
  UNION ALL
  SELECT 'closed', id, type, created,
         data__object__id, data__object__created,
         data__object__reason, data__object__status,
         data__object__amount, upper(data__object__currency),
         data__object__evidence_details__has_evidence,
         data__object__evidence_details__submission_count,
         data__object__evidence_details__due_by,
         data__previous_attributes__status,
         _ingested_at
  FROM data_bronze_stripe_prod.stripe_charge_dispute_closed
  WHERE data__object__id IN (SELECT dispute_id FROM disputes)
  UNION ALL
  SELECT 'funds_withdrawn', id, type, created,
         data__object__id, data__object__created,
         data__object__reason, data__object__status,
         data__object__amount, upper(data__object__currency),
         data__object__evidence_details__has_evidence,
         data__object__evidence_details__submission_count,
         data__object__evidence_details__due_by,
         CAST(NULL AS varchar),
         _ingested_at
  FROM data_bronze_stripe_prod.stripe_charge_dispute_funds_withdrawn
  WHERE data__object__id IN (SELECT dispute_id FROM disputes)
), dedup AS (
  SELECT dispute_id, event_created_ts, event_id, max_by(source_table, _ingested_at) AS source_table,
         max_by(event_type, _ingested_at) AS event_type,
         max_by(dispute_created_ts, _ingested_at) AS dispute_created_ts,
         max_by(reason, _ingested_at) AS reason,
         max_by(status, _ingested_at) AS status,
         max_by(amount_minor, _ingested_at) AS amount_minor,
         max_by(currency, _ingested_at) AS currency,
         max_by(has_evidence, _ingested_at) AS has_evidence,
         max_by(submission_count, _ingested_at) AS submission_count,
         max_by(due_by_ts, _ingested_at) AS due_by_ts,
         max_by(prev_status, _ingested_at) AS prev_status,
         max(_ingested_at) AS ingested_at_latest,
         count(*) AS raw_rows
  FROM unioned
  GROUP BY 1,2,3
)
SELECT dispute_id,
       from_unixtime(dispute_created_ts) AS dispute_created_utc,
       from_unixtime(event_created_ts) AS event_created_utc,
       event_type,
       event_id,
       reason,
       status,
       prev_status,
       amount_minor/100.0 AS amount,
       currency,
       has_evidence,
       submission_count,
       from_unixtime(due_by_ts) AS due_by_utc,
       raw_rows,
       ingested_at_latest
FROM dedup
ORDER BY dispute_id, event_created_ts;

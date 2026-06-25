WITH base AS (
  SELECT created AS event_created_ts, type AS event_type, data__object__id AS dispute_id,
         data__object__status AS status, data__object__reason AS reason,
         data__object__amount/100.0 AS amount, upper(data__object__currency) AS currency,
         data__object__evidence_details__has_evidence AS has_evidence,
         data__object__evidence_details__submission_count AS submission_count,
         _ingested_at
  FROM data_bronze_stripe_prod.stripe_charge_dispute_created
  WHERE data__object__id IN ('du_1SDdu0HFFv8NmmxdBfbAuUjf','du_1SDfuoHFFv8NmmxdOGVwOO9F')
  UNION ALL
  SELECT created, type, data__object__id, data__object__status, data__object__reason,
         data__object__amount/100.0, upper(data__object__currency),
         data__object__evidence_details__has_evidence,
         data__object__evidence_details__submission_count,
         _ingested_at
  FROM data_bronze_stripe_prod.stripe_charge_dispute_updated
  WHERE data__object__id IN ('du_1SDdu0HFFv8NmmxdBfbAuUjf','du_1SDfuoHFFv8NmmxdOGVwOO9F')
  UNION ALL
  SELECT created, type, data__object__id, data__object__status, data__object__reason,
         data__object__amount/100.0, upper(data__object__currency),
         data__object__evidence_details__has_evidence,
         data__object__evidence_details__submission_count,
         _ingested_at
  FROM data_bronze_stripe_prod.stripe_charge_dispute_closed
  WHERE data__object__id IN ('du_1SDdu0HFFv8NmmxdBfbAuUjf','du_1SDfuoHFFv8NmmxdOGVwOO9F')
  UNION ALL
  SELECT created, type, data__object__id, data__object__status, data__object__reason,
         data__object__amount/100.0, upper(data__object__currency),
         data__object__evidence_details__has_evidence,
         data__object__evidence_details__submission_count,
         _ingested_at
  FROM data_bronze_stripe_prod.stripe_charge_dispute_funds_withdrawn
  WHERE data__object__id IN ('du_1SDdu0HFFv8NmmxdBfbAuUjf','du_1SDfuoHFFv8NmmxdOGVwOO9F')
), ranked AS (
  SELECT *, row_number() OVER (PARTITION BY dispute_id ORDER BY event_created_ts DESC, _ingested_at DESC) AS rn
  FROM base
)
SELECT dispute_id, from_unixtime(event_created_ts) AS latest_event_utc, event_type, status, reason, amount, currency, has_evidence, submission_count
FROM ranked
WHERE rn=1
ORDER BY dispute_id;

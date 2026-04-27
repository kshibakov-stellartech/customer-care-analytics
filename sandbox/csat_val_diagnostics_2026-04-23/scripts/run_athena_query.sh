#!/usr/bin/env bash
set -euo pipefail
PROFILE="cc-analytics"
REGION="us-east-1"
WORKGROUP="primary"
OUTPUT="s3://data-stellar-athena-query-results/tmp/"
QUERY_FILE="$1"
QUERY="$(cat "$QUERY_FILE")"
QID=$(aws athena start-query-execution \
  --profile "$PROFILE" \
  --region "$REGION" \
  --work-group "$WORKGROUP" \
  --query-string "$QUERY" \
  --result-configuration "OutputLocation=$OUTPUT" \
  --query 'QueryExecutionId' \
  --output text)
for i in $(seq 1 180); do
  STATE=$(aws athena get-query-execution \
    --profile "$PROFILE" \
    --region "$REGION" \
    --query-execution-id "$QID" \
    --query 'QueryExecution.Status.State' \
    --output text)
  if [ "$STATE" = "SUCCEEDED" ]; then
    aws athena get-query-results \
      --profile "$PROFILE" \
      --region "$REGION" \
      --query-execution-id "$QID" \
      --output table
    exit 0
  elif [ "$STATE" = "FAILED" ] || [ "$STATE" = "CANCELLED" ]; then
    aws athena get-query-execution \
      --profile "$PROFILE" \
      --region "$REGION" \
      --query-execution-id "$QID" \
      --query 'QueryExecution.Status.StateChangeReason' \
      --output text
    exit 1
  fi
  sleep 1
done

echo "Timed out: $QID" >&2
exit 2

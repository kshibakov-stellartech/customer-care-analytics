#!/usr/bin/env bash
set -euo pipefail

PROFILE="cc-analytics"
REGION="us-east-1"
WORKGROUP="primary"
OUTPUT="s3://data-stellar-athena-query-results/tmp/"

run_query() {
  local query="$1"

  local qid
  qid=$(aws athena start-query-execution \
    --profile "$PROFILE" \
    --region "$REGION" \
    --work-group "$WORKGROUP" \
    --query-string "$query" \
    --result-configuration "OutputLocation=$OUTPUT" \
    --query QueryExecutionId \
    --output text)

  for _ in {1..120}; do
    local state
    state=$(aws athena get-query-execution \
      --profile "$PROFILE" \
      --region "$REGION" \
      --query-execution-id "$qid" \
      --query 'QueryExecution.Status.State' \
      --output text)

    if [[ "$state" == "SUCCEEDED" ]]; then
      aws athena get-query-results \
        --profile "$PROFILE" \
        --region "$REGION" \
        --query-execution-id "$qid" \
        --output table
      return 0
    fi

    if [[ "$state" == "FAILED" || "$state" == "CANCELLED" ]]; then
      aws athena get-query-execution \
        --profile "$PROFILE" \
        --region "$REGION" \
        --query-execution-id "$qid" \
        --query 'QueryExecution.Status.StateChangeReason' \
        --output text
      return 1
    fi

    sleep 1
  done

  echo "Timed out waiting for Athena query: $qid" >&2
  return 1
}

QUERY_PROJECTS="SELECT p.id, p.name, p.team_id, p.archived, p._fivetran_deleted, p.modified_at FROM fivetran_asana.project p WHERE lower(p.name) LIKE '%tech support%' ORDER BY p.modified_at DESC LIMIT 50"

echo "=== Step 1: find Tech Support projects ==="
run_query "$QUERY_PROJECTS"

if [[ "${1:-}" != "" ]]; then
  PROJECT_ID="$1"
  QUERY_TASKS="SELECT p.id AS project_id, p.name AS project_name, t.id AS task_id, t.name AS task_name, t.completed, t.created_at, t.completed_at, t.modified_at, t.assignee_id, u.name AS assignee_name, s.id AS section_id, s.name AS section_name, t.custom_priority, t.custom_task_status, t.custom_category, t.custom_requester, t.custom_team, t._fivetran_deleted FROM fivetran_asana.project p JOIN fivetran_asana.project_task pt ON p.id = pt.project_id JOIN fivetran_asana.task t ON pt.task_id = t.id LEFT JOIN fivetran_asana.task_section ts ON t.id = ts.task_id LEFT JOIN fivetran_asana.section s ON ts.section_id = s.id LEFT JOIN fivetran_asana.user u ON t.assignee_id = u.id WHERE p.id = '$PROJECT_ID' ORDER BY t.modified_at DESC LIMIT 200"

  echo "=== Step 2: sample tasks for project_id=$PROJECT_ID ==="
  run_query "$QUERY_TASKS"
else
  echo "Pass project_id as arg to run Step 2, example:"
  echo "  bash sandbox/asana_research/run_tech_support_probe.sh 1209882467788483"
fi

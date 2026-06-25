SELECT
  id,
  name,
  resource_subtype,
  assignee_id,
  created_by_id,
  custom_requester,
  custom_creo_source,
  custom_reo_source_status_
FROM fivetran_asana.task
WHERE id = '1212082488015300';

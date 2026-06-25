SELECT
  id,
  regexp_extract(notes, 'tickets/([0-9]+)', 1) AS ticket_id,
  notes
FROM fivetran_asana.task
WHERE id = '1212082488015300';

SELECT
  id,
  notes,
  regexp_replace(coalesce(notes, ''), '\n', ' | ') AS notes_one_line
FROM fivetran_asana.task
WHERE id = '1212082488015300';

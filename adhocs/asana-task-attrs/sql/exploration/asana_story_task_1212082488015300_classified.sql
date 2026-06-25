WITH s AS (
  SELECT created_at, type, text
  FROM fivetran_asana.story
  WHERE target_id = '1212082488015300'
), c AS (
  SELECT
    created_at,
    type,
    text,
    CASE
      WHEN regexp_like(lower(coalesce(text,'')), 'changed section from .* to .*') THEN 'section_move_changed_from_to'
      WHEN regexp_like(lower(coalesce(text,'')), 'moved this task from ".*" to ".*"') THEN 'section_move_moved_from_to'
      WHEN regexp_like(lower(coalesce(text,'')), 'changed section to ') THEN 'section_set_to'
      WHEN regexp_like(lower(coalesce(text,'')), 'added this task to ') THEN 'project_add'
      WHEN regexp_like(lower(coalesce(text,'')), 'marked this task complete') THEN 'task_completed'
      WHEN regexp_like(lower(coalesce(text,'')), 'changed completed date') THEN 'completed_date_changed'
      WHEN regexp_like(lower(coalesce(text,'')), 'changed task progress') THEN 'task_progress_changed'
      WHEN regexp_like(lower(coalesce(text,'')), 'changed the due date|removed the due date') THEN 'due_date_change'
      WHEN regexp_like(lower(coalesce(text,'')), 'added .* as a collaborator|removed .* as a collaborator') THEN 'collaborator_change'
      WHEN trim(coalesce(text,'')) = '' THEN 'empty_text'
      ELSE 'other'
    END AS event_class
  FROM s
)
SELECT event_class, count(*) AS cnt
FROM c
GROUP BY 1
ORDER BY 2 DESC;

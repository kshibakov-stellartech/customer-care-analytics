WITH base AS (
  SELECT
    s.created_at,
    s.type,
    s.text,
    s.target_id AS task_id
  FROM fivetran_asana.story s
  JOIN fivetran_asana.project_task pt
    ON pt.task_id = s.target_id
  WHERE pt.project_id = '1211305108470489'
), classified AS (
  SELECT
    CASE
      WHEN trim(coalesce(text, '')) = '' THEN 'empty_text'
      WHEN regexp_like(lower(text), 'moved this task from ".*" to ".*"') THEN 'section_move_moved_from_to'
      WHEN regexp_like(lower(text), 'changed section from .* to .*') THEN 'section_move_changed_from_to'
      WHEN regexp_like(lower(text), 'changed section to .*') THEN 'section_set_to'
      WHEN regexp_like(lower(text), 'marked this task incomplete') THEN 'task_reopened'
      WHEN regexp_like(lower(text), 'marked this task complete|completed this task') THEN 'task_completed'
      WHEN regexp_like(lower(text), 'changed task progress') THEN 'task_progress_changed'
      WHEN regexp_like(lower(text), 'changed completed date') THEN 'completed_date_changed'
      WHEN regexp_like(lower(text), 'changed the due date|removed the due date') THEN 'due_date_change'
      WHEN regexp_like(lower(text), 'added this task to .*') THEN 'project_add'
      WHEN regexp_like(lower(text), 'removed this task from .*') THEN 'project_remove'
      WHEN regexp_like(lower(text), 'added .* as a collaborator|removed .* as a collaborator') THEN 'collaborator_change'
      WHEN regexp_like(lower(text), 'added a collaborator|removed a collaborator') THEN 'collaborator_generic_change'
      WHEN regexp_like(lower(text), 'assigned to |unassigned|removed assignee') THEN 'assignee_change'
      WHEN regexp_like(lower(text), 'changed .* to .*') THEN 'field_change_generic'
      WHEN regexp_like(lower(text), 'liked your comment|liked this') THEN 'like_event'
      WHEN regexp_like(lower(text), 'commented on this task|added a comment|replied') THEN 'comment_event'
      WHEN regexp_like(lower(text), 'added an attachment|removed an attachment') THEN 'attachment_change'
      ELSE 'other'
    END AS action_type,
    created_at,
    task_id,
    text
  FROM base
)
SELECT action_type, created_at, task_id, text
FROM classified
ORDER BY action_type, created_at;

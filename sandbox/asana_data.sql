SELECT ts.*
FROM fivetran_asana.project
    LEFT JOIN fivetran_asana.project_task ON project.id = project_task.project_id
    LEFT JOIN fivetran_asana.task ON project_task.task_id = task.id
    LEFT JOIN fivetran_asana.task_section ts ON task.id = ts.task_id
WHERE 1=1
  AND project.id = '1209882467788483'
LIMIT 100
WITH
    tickets AS (
SELECT ticket_id,
       created_at as ticket_created_at,
       MAX(events__value) as requester_id
FROM data_bronze_zendesk_prod.zendesk_audit za
WHERE 1=1
  AND ticket_id = 575954
  AND events__type = 'Create'
  AND events__field_name = 'requester_id'
--AND ticket_updated_date BETWEEN DATE '2025-11-01' AND DATE '2025-11-30'
GROUP BY 1, 2
),
    agents_dict AS (
    SELECT *
    FROM (
        VALUES
            (41972533108625, 'Konstantin Shibakov', 'Admins'),
            (40215157462161, 'QA', 'Admins'),
            (39272670052113, 'Sam Bondar', 'Moon Rangers'),
            (38754864964753, 'Brian Tepliuk', 'Moon Rangers'),
            (38694917174545, 'Mike Mkrtumyan', 'Moon Rangers'),
            (38657563018769, 'Alice Sakharova', 'Moon Rangers'),
            (38022764826129, 'Allie Kostukovich', 'Blanc'),
            (38022759246737, 'Kate Rumiantseva', 'Moon Rangers'),
            (37992873903889, 'Ann Dereka', 'Moon Rangers'),
            (36064560830737, 'Mykyta', 'Admins'),
            (35310711957393, 'Anette Monaselidze', 'Blanc'),
            (35219779434897, 'Ilia Tregubov', 'Admins'),
            (34224285677201, 'Yaroslav Kukharenko', 'Admins'),
            (33602186941713, 'Jackie Si', 'Blanc'),
            (33118701264017, 'Daria Saranchova', 'Blanc'),
            (33118711659921, 'Katrina Novikova', 'Blanc'),
            (31467436910865, 'Jenny', 'Moon Rangers'),
            (30786139608081, 'Jade Kasper', 'Blanc'),
            (30655366698001, 'Catherine Moroz', 'Blanc'),
            (30648746936465, 'Alexander Petrov', 'Moon Rangers'),
            (30160506886161, 'Alex Poponin', 'Blanc'),
            (29737848444689, 'Daniel Vinokurov', 'Blanc'),
            (26440502459665, 'Nikki', 'Automation'),
            (26349132549521, 'Mia Petchenko', 'Moon Rangers'),
            (26222438547857, 'Maksym Zvieriev', 'Blanc')
    ) AS t (
        agent_id,
        agent_name,
        agent_group
    )
),
    tickets_attr AS (
SELECT ticket_id,
       tickets.ticket_created_at,
       CAST(CAST(tickets.requester_id AS DOUBLE) AS BIGINT) as requester_id,
       MAX(CASE WHEN events__field_name IN (
                                             '32351109113361', /* backoffice */
                                             '40831328206865', /* app_user_id */
                                             '32351085497873' /* supabase */
                                            )
                                        THEN events__value END
       ) as user_id,
       MAX(CASE WHEN events__type = 'Create' AND events__field_name = 'brand_id' THEN
               CASE WHEN events__value = '26467992035601' THEN 'MindScape'
                    WHEN events__value = '27810244289553' THEN 'Neurolift'
                    WHEN events__value = '26468032413713' THEN 'SmartyMe'
                    WHEN events__value = '26222456156689' THEN 'StellarTech Limited'
                    ELSE 'Unknown'
                    END
           END) as ticket_brand,
       MAX(CASE WHEN events__type = 'Create' AND events__field_name = 'ticket_form_id' THEN
               CASE WHEN events__value = '26472204214801' THEN 'main ticket form'
                    WHEN events__value = '34833592831505' THEN 'in-app ticket form'
                    WHEN events__value = '34902185196177' THEN 'test form'
                    WHEN events__value = '26222488220945' THEN 'default ticket form'
                    WHEN events__value = '35743604923281' THEN 'registration form'
                    ELSE null
                    END
           END) as ticket_form_type,
       MAX(CASE WHEN events__type = 'Create' AND events__field_name = 'requester_id' THEN channel END) as ticket_channel,
       MAX(CASE WHEN events__type = 'Create' AND events__field_name = 'subject' THEN events__value END) as ticket_subject,
       ELEMENT_AT(
           ARRAY_AGG(CASE WHEN events__value = '26222456191633' THEN 'new'
                          WHEN events__value = '26222456196881' THEN 'open'
                          WHEN events__value = '26222488415249' THEN 'in progress'
                          WHEN events__value = '26471687892881' THEN 'waiting for cs'
                          WHEN events__value = '26222456200081' THEN 'pending'
                          WHEN events__value = '26471092160657' THEN 'waiting for customer'
                          WHEN events__value = '26471128667153' THEN 'waiting for tech team'
                          WHEN events__value = '26222456206737' THEN 'solved'
                          WHEN events__value = '26471115820177' THEN 'solved (no reply)'
                         ELSE null
                     END
               ORDER BY created_at DESC, events__id DESC)
           FILTER (WHERE events__field_name = 'custom_status_id' AND events__value IS NOT NULL), 1
       ) as status,
       ELEMENT_AT(
           ARRAY_AGG(events__value ORDER BY created_at DESC, events__id DESC)
           FILTER (WHERE events__field_name = '26442658996241' AND events__value IS NOT NULL), 1
       ) as request_type,
       ELEMENT_AT(
           ARRAY_AGG(events__value ORDER BY created_at DESC, events__id DESC)
           FILTER (WHERE events__field_name = '31320582354705' AND events__value IS NOT NULL), 1
       ) as subtype
FROM tickets
    JOIN data_bronze_zendesk_prod.zendesk_audit USING(ticket_id)
WHERE 1=1
GROUP BY 1, 2, 3
),
    ticket_assignees AS (
SELECT ticket_id,
       created_at as assigned_at,
       events__value as assignee_id,
       ROW_NUMBER() OVER(PARTITION BY ticket_id ORDER BY created_at) - 1  as assignee_number
FROM tickets
    JOIN data_bronze_zendesk_prod.zendesk_audit za USING(ticket_id)
WHERE 1=1
  AND events__field_name IN ('assignee_id', 'requester_id')
  AND events__value is not null
),
    ticket_statuses AS (
SELECT t.ticket_id,
       ta.assignee_id,
       ta.assigned_at,
       created_at,
       ticket_updated_date,
       ticket_updated_at,
       events__id,
       author_id,
       lower(events__type) as action_type,
       events__field_name,
       CASE WHEN events__field_name = 'custom_status_id' THEN
                CASE WHEN events__value = '26222456191633' THEN 'new'
                     WHEN events__value = '26222456196881' THEN 'open'
                     WHEN events__value = '26222488415249' THEN 'in progress'
                     WHEN events__value = '26471687892881' THEN 'waiting for cs'
                     WHEN events__value = '26222456200081' THEN 'pending'
                     WHEN events__value = '26471092160657' THEN 'waiting for customer'
                     WHEN events__value = '26471128667153' THEN 'waiting for tech team'
                     WHEN events__value = '26222456206737' THEN 'solved'
                     WHEN events__value = '26471115820177' THEN 'solved (no reply)'
                    ELSE null
                END
           ELSE events__value
       END as events__value,
       ROW_NUMBER() over(PARTITION BY t.ticket_id, za.created_at ORDER BY events__field_name) as status_rn
FROM tickets t
    JOIN data_bronze_zendesk_prod.zendesk_audit za ON t.ticket_id = za.ticket_id
    LEFT JOIN ticket_assignees ta ON t.ticket_id = ta.ticket_id
                                 AND CAST(CAST(za.author_id AS DOUBLE) AS BIGINT) = CAST(CAST(ta.assignee_id AS DOUBLE) AS BIGINT)
WHERE 1=1
  AND events__field_name IN (
                             'custom_status_id',
                             'status'
                            )
),

    ticket_log_raw AS (
SELECT --attr.*,
       t.ticket_id,
       t.requester_id,
       t.ticket_created_at,
       t.ticket_brand,
       t.ticket_channel,
       t.ticket_form_type,
       t.request_type,
       t.subtype,
       t.ticket_subject,
       ta.assigned_at,
       CASE WHEN events__field_name = 'assignee_id' THEN CAST(CAST(events__value AS DOUBLE) AS BIGINT)
            WHEN ta.assignee_id is null THEN CAST(-1 AS BIGINT)
            ELSE CAST(CAST(ta.assignee_id AS DOUBLE) AS BIGINT)
       END as assignee_id,
       ta.assignee_number,
       created_at,
       CASE WHEN events__type = 'Notification' OR (events__field_name = 'assignee_id') THEN -1 ELSE author_id END as author_id,
       CASE WHEN events__field_name = 'requester_id' THEN 'ticket created'
            WHEN events__field_name = 'assignee_id' THEN 'agent assigned'
            WHEN events__type NOT IN ('Comment', 'Notification') THEN events__field_name
           ELSE lower(events__type)
       END as action_type,
       CASE WHEN events__field_name = 'custom_status_id' THEN
                CASE WHEN events__value = '26222456191633' THEN 'new'
                     WHEN events__value = '26222456196881' THEN 'open'
                     WHEN events__value = '26222488415249' THEN 'in progress'
                     WHEN events__value = '26471687892881' THEN 'waiting for cs'
                     WHEN events__value = '26222456200081' THEN 'pending'
                     WHEN events__value = '26471092160657' THEN 'waiting for customer'
                     WHEN events__value = '26471128667153' THEN 'waiting for tech team'
                     WHEN events__value = '26222456206737' THEN 'solved'
                     WHEN events__value = '26471115820177' THEN 'solved (no reply)'
                    ELSE null
                END
            WHEN events__type = 'Notification' THEN events__from_title
           ELSE COALESCE(events__value, events__body)
       END as events__value,
       CASE WHEN events__field_name = 'custom_status_id' THEN DATE_DIFF('second', CASE WHEN ta.assignee_number = 1 THEN t.ticket_created_at ELSE ta.assigned_at END, created_at) END as proccessing_time
FROM tickets_attr t
    JOIN data_bronze_zendesk_prod.zendesk_audit za ON t.ticket_id = za.ticket_id
    LEFT JOIN ticket_assignees ta ON t.ticket_id = ta.ticket_id
                                 AND CAST(CAST(za.author_id AS DOUBLE) AS BIGINT) = CAST(CAST(ta.assignee_id AS DOUBLE) AS BIGINT)
    --JOIN tickets_attr attr USING(ticket_id)
WHERE 1=1
  AND (
        events__field_name = 'custom_status_id'
        OR
        (events__field_name = 'assignee_id' AND events__value is not null)
        OR
        (events__field_name = 'status' AND events__value = 'closed')
        OR
        events__type IN (
                         'Comment',
                         'Notification'
                        )
        )
)

SELECT *
FROM ticket_assignees

;
SELECT *
FROM ticket_log_raw lr
WHERE 1=1
  --AND assignee_id <> requester_id
ORDER BY created_at, CASE action_type WHEN 'agent assigned' THEN 1
                                      WHEN 'custom_status_id' THEN 3
                                      WHEN 'notification' THEN 2
                                      WHEN 'comment' THEN 0
                                      WHEN 'status' THEN 4
                     END



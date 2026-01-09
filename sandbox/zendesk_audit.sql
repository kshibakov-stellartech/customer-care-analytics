SELECT ticket_id,
       created_at,
       ticket_updated_date,
       ticket_updated_at,
       events__id,
       channel,
       author_id,
       events__author_id,
       events__type,
       events__field_name,
       events__value,
       events__previous_value,
       events__body,
       events__public,
       events__type,
       events__channel,
       events__subject,
       events__from_title
FROM data_bronze_zendesk_prod.zendesk_audit
WHERE ticket_id = 612356
/*  AND events__type IN (
                        'Comment'
                      )*/
  --AND events__field_name = '40831328206865'
  /*
  AND events__type IN (
                        --'ChatStartedEvent'
                        --,'ChatEndedEvent'
                      )
  */
ORDER BY created_at, events__id
--LIMIT 10
;

SELECT *
FROM (
SELECT ticket_id,
       created_at,
       ticket_updated_date,
       ticket_updated_at,
       events__field_name,
       events__type,
       events__value,
       events__previous_value,
       LAG(created_at, 1) over(PARTITION BY ticket_id ORDER BY created_at) as prev_action,
       DATE_DIFF('second', LAG(created_at, 1) over(PARTITION BY ticket_id ORDER BY created_at), created_at) as proccessing_time
FROM data_bronze_zendesk_prod.zendesk_audit za
WHERE ticket_id = 617149
  AND events__field_name = 'assignee_id'
UNION ALL
SELECT ticket_id,
       created_at,
       ticket_updated_date,
       ticket_updated_at,
       events__field_name,
       events__type,
       events__value,
       events__previous_value,
       LAG(created_at, 1) over(PARTITION BY ticket_id ORDER BY created_at) as prev_action,
       DATE_DIFF('second', LAG(created_at, 1) over(PARTITION BY ticket_id ORDER BY created_at), created_at) as proccessing_time
FROM data_bronze_zendesk_prod.zendesk_audit za
WHERE ticket_id = 593604
  AND events__field_name = 'status'
) q
ORDER BY created_at

;
/*
request type 26442658996241
subtype 31320582354705
type 26222456040337
reason 31337154687889
resolution type 40653666145041
ticket outcome 31290621986705
mail
    30971824463761 auth
    30971823749777 contact
custom_status_id
    26222456191633 new
    26222456196881 open
    26222488415249 in progress
    26471687892881 waiting for cs
    26222456200081 pending
    26471092160657 waiting for customer
    26471128667153 waiting for tech team
    26222456206737 solved
    26471115820177 solved (no reply)
*/

;

WITH agents_dict AS (
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
            (26440502459665, 'Nikki', 'Admins'),
            (26349132549521, 'Mia Petchenko', 'Moon Rangers'),
            (26222438547857, 'Maksym Zvieriev', 'Blanc')
    ) AS t (
        agent_id,
        agent_name,
        agent_group
    )
)

SELECT *
FROM agents_dict
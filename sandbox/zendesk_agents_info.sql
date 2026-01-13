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
WHERE agent_id IN (
41688092086417, -- user?
26440502459665,
38754864964753
    )
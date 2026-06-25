
WITH input_emails AS (
    SELECT *
    FROM (
      VALUES
        (1, 'lhupje@yahoo.co.uk', '449549'),
        (2, 'marianazregan@gmail.com', '462233'),
        (3, 'litzy1aragon@yahoo.com', '471928'),
        (4, 'aspainter7@gmail.com', '474794'),
        (5, 'kddvt53@gmail.com', '482029'),
        (6, 'jweibel423@yahoo.com', '441961'),
        (7, 'jweibel423@yahoo.com', '441954'),
        (8, 'lidobeach@icloud.com', '442109'),
        (9, 'lidobeach@icloud.com', '442110'),
        (10, 'hcampos@windermereca.com', '442244'),
        (11, 'csalamancaq@gmail.com', '483355'),
        (12, 'jennifershay@myyahoo.com', '443026'),
        (13, 'nataliabilko3@gmail.com', '444149'),
        (14, 'thomsk1@verizon.net', '444612'),
        (15, 'lhupje@yahoo.co.uk', '449548'),
        (16, 'tlc63@me.com', '449653'),
        (17, 'jonathanhamze@icloud.com', '449946'),
        (18, 'ziomara.prera@gmail.com', '450030'),
        (19, 'marianazregan@gmail.com', '450926'),
        (20, 'zoe.m.martin72@gmail.com', '453063'),
        (21, 'tlc63@me.com', '453530'),
        (22, 'madinahabibzi@gmail.com', '453438'),
        (23, 'madinahabibzi@gmail.com', '453436'),
        (24, 'dyna.sok@hotmail.com', '450102'),
        (25, 'lkastahova@yahoo.com', '453694'),
        (26, 'metalzbme@yahoo.com', '453769'),
        (27, 'mjstrube@gmail.com', '489984'),
        (28, 'metalzbme@yahoo.com', '454785'),
        (29, 'metalzbme@yahoo.com', '454780'),
        (30, 'garciabrenda3@yahoo.com', '454824'),
        (31, 'devon@invokbrands.com', '454934'),
        (32, 'devon@invokbrands.com', '454935'),
        (33, 'tvbryant@comcast.net', '456025'),
        (34, 'kabukaholdings@gmail.com', '457257'),
        (35, 'info@isabelsanogueira.com', '457813'),
        (36, 'axm0307@gmail.com', '492316'),
        (37, 'taniadeserio@yahoo.com', '458377'),
        (38, 'lkastahova@yahoo.com', '459379'),
        (39, 'gustavochiu@gmail.com', '459855'),
        (40, 'ebilbao10@aol.com', '461096'),
        (41, 'laquilino5@gmail.com', '462327'),
        (42, 'kseniashnyra@me.com', '463591'),
        (43, 'pbrsingh@iprimus.com.au', '463786'),
        (44, 'myrlandemathias@yahoo.com', '463855'),
        (45, 'chsoans@gmail.com', '464246'),
        (46, 'apwmth@gmail.com', '465332'),
        (47, 'kinjalf10@gmail.com', '465778'),
        (48, 'kinjalf10@gmail.com', '465782'),
        (49, 'chevelleross@icloud.com', '466656'),
        (50, 'chevelleross@icloud.com', '466647'),
        (51, 'ydavis05@outlook.com', '466890'),
        (52, 'ydavis05@outlook.com', '466893'),
        (53, 'tansue81@yahoo.com', '468555'),
        (54, 'tansue81@yahoo.com', '468824'),
        (55, 'kdaly0912@yahoo.com', '470602'),
        (56, 'mki1108@gmail.com', '470613'),
        (57, 'mki1108@gmail.com', '470615'),
        (58, 'mleighs@me.com', '470839'),
        (59, 'litzy1aragon@yahoo.com', '471926'),
        (60, 'bethannnissen@gmail.com', '502737'),
        (61, 'monicaluna18@gmail.com', '472169'),
        (62, 'msmcneary@gmail.com', '472535'),
        (63, 'farahqabraham@gmail.com', '473027'),
        (64, 'bri_contoli49@hotmail.com', '473306'),
        (65, 'bri_contoli49@hotmail.com', '473307'),
        (66, 'bri_contoli49@hotmail.com', '473309'),
        (67, 'bri_contoli49@hotmail.com', '473310'),
        (68, 'bri_contoli49@hotmail.com', '473311'),
        (69, 'bri_contoli49@hotmail.com', '473308'),
        (70, '3nicoles@gmail.com', '501783'),
        (71, 'tvbryant@comcast.net', '473449'),
        (72, 'aracelinava1@yahoo.com', '473563'),
        (73, 'aracelinava1@yahoo.com', '473603'),
        (74, 'mayu8777@yahoo.com', '505966'),
        (75, 'aspainter7@gmail.com', '474878'),
        (76, 'aspainter7@gmail.com', '474791'),
        (77, 'michelefowlerart@gmail.com', '474944'),
        (78, 'jfwong1@gmail.com', '506781'),
        (79, 'titablaga@icloud.com', '476164'),
        (80, 'janetleevaldez@aol.com', '476359'),
        (81, 'heidi_shir@yahoo.com', '476505'),
        (82, 'heidi_shir@yahoo.com', '476504'),
        (83, 'terripembleton@gmail.com', '476888'),
        (84, 'simahalisse@msn.com', '477292'),
        (85, 'amonissalon@yahoo.com', '477884'),
        (86, 'elankila@yahoo.com', '512516'),
        (87, 'elankila@yahoo.com', '512526'),
        (88, 'brittanyjoyner364@gmail.com', '478544'),
        (89, 'pierredessert@me.com', '512620'),
        (90, 'amelia.baker@hotmail.com', '478760'),
        (91, 'louise.lake@hotmail.co.uk', '478895'),
        (92, 'claudiacazanas@aol.com', '479127'),
        (93, 'gaedetaylor@yahoo.com', '479313'),
        (94, 'moonaaak1234@gmail.com', '479479'),
        (95, 'haley.grizzaffi@gmail.com', '479823'),
        (96, 'lynette.bishop@gmsil.vom', '479828'),
        (97, 'rimaasally@gmail.com', '480106'),
        (98, 'eleisetheuer@gmail.com', '515634'),
        (99, 'james@msselainelancaster.com', '480296'),
        (100, 'rgill04@gmail.com', '480299')
    ) AS t(row_num, email, ticket_id)
),
charge_best AS (
    SELECT data__object__id, psp_account_name,
           lower(data__object__billing_details__email) AS billing_email,
           lower(data__object__metadata__customer_email) AS metadata_email,
           row_number() OVER (PARTITION BY psp_account_name, data__object__id ORDER BY from_unixtime(created) DESC) AS rn
    FROM data_bronze_stripe_prod.stripe_charge_succeeded
),
disputes AS (
    SELECT DISTINCT
      lower(COALESCE(dc.data__object__evidence__customer_email_address, cb.billing_email, cb.metadata_email)) AS user_email
    FROM data_bronze_stripe_prod.stripe_charge_dispute_created dc
    LEFT JOIN charge_best cb
      ON dc.data__object__charge = cb.data__object__id
     AND dc.psp_account_name = cb.psp_account_name
     AND cb.rn = 1
    WHERE COALESCE(dc.data__object__evidence__customer_email_address, cb.billing_email, cb.metadata_email) IS NOT NULL
),
status AS (
    SELECT i.*, CASE WHEN d.user_email IS NOT NULL THEN 1 ELSE 0 END AS is_found
    FROM input_emails i
    LEFT JOIN disputes d ON i.email = d.user_email
)

SELECT row_num, email, ticket_id
FROM status
WHERE is_found = 0
ORDER BY row_num
LIMIT 20;

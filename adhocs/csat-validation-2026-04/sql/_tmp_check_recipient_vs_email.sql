WITH sample AS (
  SELECT * FROM (
    VALUES
      (CAST(449549 AS bigint), 'lhupje@yahoo.co.uk'),
      (CAST(462233 AS bigint), 'marianazregan@gmail.com'),
      (CAST(471928 AS bigint), 'litzy1aragon@yahoo.com'),
      (CAST(482029 AS bigint), 'kddvt53@gmail.com'),
      (CAST(441961 AS bigint), 'jweibel423@yahoo.com')
  ) AS t(ticket_id_csv, email_csv)
)
SELECT
  s.ticket_id_csv,
  s.email_csv,
  z.ticket_id AS ticket_id_zd,
  z.created_at,
  z.recipient,
  z.requester_id,
  z.subject
FROM sample s
LEFT JOIN data_bronze_zendesk_prod.zendesk_tickets z
  ON z.ticket_id = s.ticket_id_csv
ORDER BY s.ticket_id_csv;

SELECT *
FROM data_bronze_adapty_prod.adapty_events_export
WHERE 1=1
  --AND transaction_id = '100002658927457'
  AND regexp_like(attributes,'''email''\s*:\s*''nikolai51502004@yahoo\.com''')
LIMIT 100
;
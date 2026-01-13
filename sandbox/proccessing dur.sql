;
     ,
    author_block_duration AS (
SELECT ticket_id,
       author_id,
       created_at,
       SUM(is_new_block) OVER (PARTITION BY ticket_id ORDER BY created_at) AS block_id
FROM (
    SELECT ticket_id,
           author_id,
           created_at,
            CASE
                WHEN author_id != LAG(author_id) OVER (PARTITION BY ticket_id ORDER BY created_at)
                     OR LAG(author_id) OVER (PARTITION BY ticket_id ORDER BY created_at) IS NULL
                THEN 1
                ELSE 0
            END AS is_new_block
    FROM tickets
        JOIN data_bronze_zendesk_prod.zendesk_audit USING(ticket_id)
    WHERE 1=1
      AND author_id <> -1
     ) bd
)
    SELECT *
    FROM author_block_duration
;
     ,
    author_total_duration_diffs AS (
    SELECT ticket_id,
           author_id,
           block_id,
           date_diff('second', MIN(created_at), MAX(created_at)) AS block_duration_seconds
    FROM author_block_duration
    GROUP BY 1, 2, 3
)

SELECT *
FROM author_total_duration_diffs

;,
    author_total_duration AS (
SELECT ticket_id,
       author_id,
       SUM()
FROM
)

     ;
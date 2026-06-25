WITH ids AS (
  SELECT * FROM (
    VALUES
      'PP-R-AYO-596018258',
      'PP-R-NOA-596248579',
      'PP-R-UPU-596345837'
  ) AS t(dispute_id)
)
SELECT 'dispute' AS src, d.*
FROM fivetran_paypal_prod.dispute d
JOIN ids i ON d.id = i.dispute_id
ORDER BY d.id;

SELECT 'disputed_transaction' AS src, dt.*
FROM fivetran_paypal_prod.disputed_transaction dt
JOIN ids i ON dt.dispute_id = i.dispute_id
ORDER BY dt.dispute_id;

SELECT 'adjudication' AS src, a.*
FROM fivetran_paypal_prod.adjudication a
JOIN ids i ON a.dispute_id = i.dispute_id
ORDER BY a.dispute_id, a.create_time;

SELECT 'evidence' AS src, e.*
FROM fivetran_paypal_prod.evidence e
JOIN ids i ON e.dispute_id = i.dispute_id
ORDER BY e.dispute_id, e.create_time;

SELECT 'dispute_message' AS src, m.*
FROM fivetran_paypal_prod.dispute_message m
JOIN ids i ON m.dispute_id = i.dispute_id
ORDER BY m.dispute_id, m.create_time;

SELECT 'money_movement' AS src, mm.*
FROM fivetran_paypal_prod.money_movement mm
JOIN ids i ON mm.dispute_id = i.dispute_id
ORDER BY mm.dispute_id, mm.create_time;

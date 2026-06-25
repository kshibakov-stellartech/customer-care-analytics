SELECT *
FROM fivetran_paypal_prod.evidence
WHERE dispute_id IN ('PP-R-AYO-596018258','PP-R-NOA-596248579','PP-R-UPU-596345837')
ORDER BY dispute_id, index;

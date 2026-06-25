SELECT subscription_id,
       sub_status,
       subscription_created_at,
       subscription_canceled_at,
       subscription_updated_at,
       cs.application_name,
       email
FROM data_silver_chargebee_flow_prod.chargebee_subscription cs
    LEFT JOIN data_silver_chargebee_flow_prod.chargebee_customer cc ON cs.vendor_customer_id = cc.customer_id
WHERE 1=1
  AND sub_status = 'active'
  AND email LIKE '%stellarlab.tech%'

;
WITH
    dispute_list AS (
SELECT *
FROM (
    VALUES
        ('du_1TMK5BJ57MVrbnpuFp2SkLhV'),
        ('du_1TK9TJGPcdf0eVpx1MsJGcPu'),
        ('du_1TALqJJ57MVrbnpunZtcO8FI')
) AS t(dispute_id)
),
    dispute_created AS (
SELECT
data__object__id as dispute_id,
data__object__payment_intent as payment_intent_id,
data__object__charge as charge_id,
data__object__payment_method_details__card__brand as card_brand
FROM data_bronze_stripe_prod.stripe_charge_dispute_created dc
    JOIN dispute_list dl ON dc.data__object__id = dl.dispute_id
),
    charge_succeeded AS (
SELECT
dc.*,
data__object__authorization_code as authorization_code,
data__object__payment_method_details__card__network_transaction_id as network_transaction_id,
data__object__payment_method_details__card__country as card_country,
data__object__payment_method_details__card__issuer as card_issuer,
data__object__payment_method_details__card__checks__cvc_check as cvc_check,
data__object__billing_details__address__postal_code as postal_code,
data__object__payment_method_details__card__checks__address_postal_code_check as postal_code_check,
data__object__customer as customer
FROM data_bronze_stripe_prod.stripe_charge_succeeded cc
JOIN dispute_created dc ON cc.data__object__id = dc.charge_id
),
    filtered AS (
    SELECT
        details__execution__provider_reference,
        details__action,
        details__payment_composition
    FROM firehose_payrails_webhook_prod.payrails p
        JOIN dispute_created dc ON p.details__execution__provider_reference = dc.payment_intent_id
    WHERE 1 = 1
      AND details__action = 'capture'
),

flat AS (
    SELECT
        t.details__execution__provider_reference,
        u.payment_composition_index,
        p
    FROM filtered t
    LEFT JOIN UNNEST(t.details__payment_composition)
        WITH ORDINALITY AS u(p, payment_composition_index)
        ON TRUE
)

SELECT
    cs.*,
    p.payment_instrument__data__expiry_month as expiry_month,
    p.payment_instrument__data__expiry_year as expiry_year,
    p.payment_instrument__data__suffix as token_last_4,
    p.payment_instrument__display_name as display_name,
    p.payment_instrument__data__network as network,
    p.payment_instrument__description as tokenization_method,
    p.payment_instrument__fingerprint as fingerprint,
    p.payment_instrument__network_transaction_reference,
    p.payment_instrument__payment_method,
    p.amount__currency,
    p.amount__value,
    p.payment_id,
    p.payment_instrument__data__bin_lookup__type
FROM charge_succeeded cs
    LEFT JOIN flat p ON cs.payment_intent_id = p.details__execution__provider_reference
;

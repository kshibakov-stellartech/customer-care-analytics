/*
нужны данные по первой успешной транзакции по каждому юзеру - там больше инфо
zip check czc chexk - payment method info
*/

WITH dispute_list AS (
SELECT *
FROM (
    VALUES
        ('du_1TMK5BJ57MVrbnpuFp2SkLhV'),
        ('du_1TK9TJGPcdf0eVpx1MsJGcPu'),
        ('du_1TALqJJ57MVrbnpunZtcO8FI'),
        ('du_1TTDO3J57MVrbnpu1LtJ4xxt')
) AS t(dispute_id)
),
    dispute_created AS (
SELECT
data__object__id as dispute_id,
data__object__payment_intent as payment_intent,
data__object__balance_transaction as transaction_id,
data__object__charge as charge_id,
data__object__payment_method_details__card__brand as card_brand
FROM data_bronze_stripe_prod.stripe_charge_dispute_created dc
    JOIN dispute_list dl ON dc.data__object__id = dl.dispute_id
),
    charge_customer AS (
SELECT data__object__id,
       data__object__customer as customer,
       data__object__authorization_code,
       data__object__payment_method_details__card__authorization_code,
       COALESCE(data__object__authorization_code,
       data__object__payment_method_details__card__authorization_code) AS authorization_code
FROM data_bronze_stripe_prod.stripe_charge_succeeded cc
    JOIN dispute_created dc ON cc.data__object__id = dc.charge_id
)

SELECT * FROM charge_customer
;     ,

    charge_succeeded AS (
SELECT
cc.*
/*dc.*,
data__object__id as charge_id,
data__object__customer as customer,
data__object__payment_method_details__card__network_transaction_id as network_transaction_id,
data__object__payment_method_details__card__country as card_country,
data__object__payment_method_details__card__issuer as card_issuer,
data__object__payment_method_details__card__network_token__used as network_token,
data__object__payment_method_details__card__fingerprint as fingerprint,
data__object__payment_method_details__card__checks__cvc_check as cvc_check,
data__object__billing_details__address__postal_code as postal_code,
data__object__payment_method_details__card__checks__address_postal_code_check as postal_code_check*/
FROM data_bronze_stripe_prod.stripe_charge_succeeded cc
    JOIN charge_customer chc ON cc.data__object__customer = chc.customer
),
    filtered AS (
    SELECT
        details__execution__provider_reference,
        details__action,
        details__payment_composition
    FROM firehose_payrails_webhook_prod.payrails p
        JOIN charge_succeeded cs ON p.details__execution__provider_reference = cs.data__object__payment_intent
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
),
    payrails_flat AS (
SELECT
    p.payment_instrument__data__suffix as token_last_4,
    p.payment_instrument__data__expiry_month,
    p.payment_instrument__data__expiry_year,
    p.payment_instrument__data__network,
    p.payment_instrument__data__payer_id,
    p.payment_instrument__data__payment_account_reference,
    p.payment_instrument__default,
    p.payment_instrument__description as tokenization_method,
    p.payment_instrument__display_name,
    p.payment_instrument__fingerprint,
    p.payment_instrument__holder_id,
    p.payment_instrument__network_transaction_reference,
    p.payment_instrument__payment_method,
    details__execution__provider_reference,
    -- all payment_composition row fields
    p.amount__currency,
    p.amount__value,
    p.authorization_code,
    p.payment_id,

    p.payment_instrument__created_at,
    p.payment_instrument__data__bin,
    p.payment_instrument__data__bin_lookup__bin,
    p.payment_instrument__data__bin_lookup__issuer,
    p.payment_instrument__data__bin_lookup__issuer_country__code,
    p.payment_instrument__data__bin_lookup__issuer_country__iso3,
    p.payment_instrument__data__bin_lookup__issuer_country__name,
    p.payment_instrument__data__bin_lookup__network,
    p.payment_instrument__data__bin_lookup__segment,
    p.payment_instrument__data__bin_lookup__type,
    -- selected fields from payment_token JSON
    json_extract_scalar(
        try(json_parse(p.payment_instrument__data__payment_token)),
        '$.token.paymentMethod.displayName'
    ) AS payment_token__payment_method__display_name,

    json_extract_scalar(
        try(json_parse(p.payment_instrument__data__payment_token)),
        '$.token.paymentMethod.network'
    ) AS payment_token__payment_method__network,

    json_extract_scalar(
        try(json_parse(p.payment_instrument__data__payment_token)),
        '$.token.paymentMethod.type'
    ) AS payment_token__payment_method__type,

    json_extract_scalar(
        try(json_parse(p.payment_instrument__data__payment_token)),
        '$.token.transactionIdentifier'
    ) AS payment_token__transaction_identifier,

    json_extract_scalar(
        try(json_parse(p.payment_instrument__data__payment_token)),
        '$.token.paymentData.version'
    ) AS payment_token__payment_data__version,

    json_extract_scalar(
        try(json_parse(p.payment_instrument__data__payment_token)),
        '$.token.paymentData.header.publicKeyHash'
    ) AS payment_token__payment_data__header__public_key_hash,

    json_extract_scalar(
        try(json_parse(p.payment_instrument__data__payment_token)),
        '$.token.paymentData.header.ephemeralPublicKey'
    ) AS payment_token__payment_data__header__ephemeral_public_key,

    json_extract_scalar(
        try(json_parse(p.payment_instrument__data__payment_token)),
        '$.token.paymentData.header.transactionId'
    ) AS payment_token__payment_data__header__transaction_id,

    json_extract_scalar(
        try(json_parse(p.payment_instrument__data__payment_token)),
        '$.token.paymentData.data'
    ) AS payment_token__payment_data__data,

    json_extract_scalar(
        try(json_parse(p.payment_instrument__data__payment_token)),
        '$.token.paymentData.signature'
    ) AS payment_token__payment_data__signature,

    -- tokens array(row(...)) split into columns, preserving one row per payment_composition item
    array_join(
        transform(
            p.payment_composition__payment_instrument__tokens,
            x -> x.reference
        ),
        ', '
    ) AS payment_composition__payment_instrument__token_references,

    array_join(
        transform(
            p.payment_composition__payment_instrument__tokens,
            x -> x.type
        ),
        ', '
    ) AS payment_composition__payment_instrument__token_types,

    array_join(
        transform(
            p.payment_composition__payment_instrument__tokens,
            x -> x.meta__holder_reference
        ),
        ', '
    ) AS payment_composition__payment_instrument__token_meta__holder_references,

    element_at(p.payment_composition__payment_instrument__tokens, 2).reference
        AS payment_composition__payment_instrument__token_2__reference,

    element_at(p.payment_composition__payment_instrument__tokens, 2).type
        AS payment_composition__payment_instrument__token_2__type,

    element_at(p.payment_composition__payment_instrument__tokens, 2).meta__holder_reference
        AS payment_composition__payment_instrument__token_2__meta__holder_reference,

    p.payment_instrument__updated_at,
    p.payment_instrument_id,
    p.payment_instrument_token__meta__holder_reference,
    p.payment_instrument_token__reference,
    p.payment_instrument_token__type,
    p.payment_instrument_token_id,
    p.payment_method_code,

    p.provider__created_at,
    p.provider__display_name,
    p.provider__id,
    p.provider__name,
    p.provider__notification_url_template,
    p.provider__status,
    p.provider__type,
    p.provider__updated_at,
    p.provider_config_id,
    p.provider_id,
    p.provider_reference,
    p.reason,
    p.reason_description,
    p.retries__completed_attempts,
    p.retries__idempotency_key,
    p.retries__next_scheduled_at,
    p.retries__started_at,
    p.store_instrument,
    p.success,
    p.three_ds__authentication_type,
    p.three_ds__authentication_value,
    p.three_ds__cavv_algorithm,
    p.three_ds__directory_response,
    p.three_ds__ds_trans_id,
    p.three_ds__eci,
    p.three_ds__liability_shifted,
    p.three_ds__trans_status,
    p.three_ds__trans_status_reason,
    p.three_ds__version,
    p.provider_response_additional_fields__purchase_units
FROM flat
)

SELECT *
FROM charge_succeeded cs
    LEFT JOIN payrails_flat pf ON cs.data__object__payment_intent = pf.details__execution__provider_reference
;


/*
2. authorization_code
   6-символьный код авторизации от банка-эмитента карты.
   Источник: payment_method_details.card.authorization_code

5. physical_last_4
   Последние 4 цифры реальной funding card, если доступны.

12. initial_enrollment_date
        можно найти
    Timestamp первой успешной транзакции по customer profile / holderReference.

13. initial_verification_status
        можно найти
    Показывает, прошла ли первая транзакция 3DS или CVC-проверку.
*/
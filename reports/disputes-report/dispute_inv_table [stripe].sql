WITH dispute_list AS (
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
data__object__payment_intent as payment_intent,
data__object__charge as charge_id,
data__object__payment_method_details__card__brand as card_brand
FROM data_bronze_stripe_prod.stripe_charge_dispute_created dc
    JOIN dispute_list dl ON dc.data__object__id = dl.dispute_id
),
    charge_succeeded AS (
SELECT
dc.*,
data__object__authorization_code,
data__object__payment_method_details__card__network_transaction_id as network_transaction_id,
data__object__payment_method_details__card__country as card_country,
data__object__payment_method_details__card__issuer as card_issuer,
data__object__payment_method_details__card__checks__cvc_check as cvc_check,
data__object__billing_details__address__postal_code as postal_code,
data__object__payment_method_details__card__checks__address_postal_code_check as postal_code_check,
data__object__customer as customer
FROM data_bronze_stripe_prod.stripe_charge_succeeded cc
JOIN dispute_created dc ON cc.data__object__id = dc.charge_id
)

SELECT *
FROM charge_succeeded
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
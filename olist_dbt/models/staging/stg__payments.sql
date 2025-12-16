{{ config(
    unique_key='payment_id_surrogate' 
) }}

WITH source AS (
    SELECT *
    FROM {{ source('dwh', 'olist_order_payments_dataset') }}
),

orders_raw AS (
    SELECT
        order_id,
        order_purchase_timestamp
    FROM {{ source('dwh', 'olist_orders_dataset') }}
),

transformed AS (
    SELECT

        CAST(s.order_id || '-' || s.payment_sequential AS VARCHAR(100))
            AS payment_id_surrogate,
        CAST(s.order_id AS VARCHAR(50)) AS order_id,
        CAST(s.payment_sequential AS INT) AS payment_sequential,
        CAST(s.payment_type AS VARCHAR(20)) AS payment_type,
        CAST(s.payment_installments AS INT) AS payment_installments,
        CAST(s.payment_value AS DOUBLE PRECISION) AS payment_value,
        CAST(o.order_purchase_timestamp AS TIMESTAMP)
            AS order_purchase_timestamp
    FROM source AS s
    INNER JOIN orders_raw AS o ON s.order_id = o.order_id
)

SELECT * FROM transformed

{% if is_incremental() %}
    WHERE
        order_purchase_timestamp
        > (SELECT MAX(order_purchase_timestamp) FROM {{ this }})
{% endif %}

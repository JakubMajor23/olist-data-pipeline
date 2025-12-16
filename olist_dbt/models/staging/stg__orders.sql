{{ config(
    unique_key='order_id'
) }}

WITH source AS (
    SELECT *
    FROM {{ source('dwh', 'olist_orders_dataset') }}
),

transformed AS (

    SELECT
        CAST(order_id AS VARCHAR(50)) AS order_id,
        CAST(customer_id AS VARCHAR(50)) AS customer_id,
        CAST(
            CASE
                WHEN
                    order_status = 'delivered'
                    AND (
                        order_delivered_carrier_date IS NULL
                        OR order_delivered_customer_date IS NULL
                    )
                    THEN 'shipped'
                ELSE order_status
            END AS VARCHAR(20)
        ) AS order_status,
        CAST(
            CASE
                WHEN order_approved_at IS NULL AND order_status = 'delivered'
                    THEN order_purchase_timestamp
                ELSE order_approved_at
            END AS TIMESTAMP
        ) AS order_approved_at,

        COALESCE(
            order_approved_at IS NULL AND order_status = 'delivered', FALSE
        ) AS is_approval_date_imputed,

        CAST(order_purchase_timestamp AS TIMESTAMP) AS order_purchase_timestamp,
        CAST(order_estimated_delivery_date AS DATE)
            AS order_estimated_delivery_date,
        CAST(order_delivered_carrier_date AS TIMESTAMP)
            AS order_delivered_carrier_date,
        CAST(order_delivered_customer_date AS TIMESTAMP)
            AS order_delivered_customer_date

    FROM source
)

SELECT *
FROM transformed

{% if is_incremental() %}
    WHERE
        order_purchase_timestamp
        > (SELECT MAX(order_purchase_timestamp) FROM {{ this }})
{% endif %}

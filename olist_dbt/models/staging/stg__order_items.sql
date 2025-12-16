{{ config(
    materialized='incremental',
    unique_key='order_item_surrogate_key' 
) }}


{% set max_timestamp %}
  (SELECT MAX(order_purchase_timestamp) FROM {{ this }})
{% endset %}

WITH source_items AS (
    SELECT * FROM {{ source('dwh', 'olist_order_items_dataset') }}
),

source_orders AS (
    SELECT
        order_id,
        order_purchase_timestamp
    FROM {{ source('dwh', 'olist_orders_dataset') }}

    {% if is_incremental() %}
        WHERE order_purchase_timestamp > {{ max_timestamp }}
    {% endif %}
),

joined_data AS (
    SELECT
        CAST(
            i.order_id
            || '-'
            || CAST(i.order_item_id AS VARCHAR) AS VARCHAR(100)
        ) AS order_item_surrogate_key,

        CAST(i.order_id AS VARCHAR(50)) AS order_id,
        CAST(i.order_item_id AS INT) AS order_item_id,
        CAST(i.product_id AS VARCHAR(50)) AS product_id,
        CAST(i.seller_id AS VARCHAR(50)) AS seller_id,
        CAST(i.shipping_limit_date AS TIMESTAMP) AS shipping_limit_date,
        CAST(i.price AS DOUBLE PRECISION) AS price,
        CAST(i.freight_value AS DOUBLE PRECISION) AS freight_value,
        CAST(o.order_purchase_timestamp AS TIMESTAMP)
            AS order_purchase_timestamp

    FROM source_items AS i
    INNER JOIN source_orders AS o ON i.order_id = o.order_id
)

SELECT * FROM joined_data

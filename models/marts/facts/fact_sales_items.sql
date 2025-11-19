WITH stg_order_items AS (
    SELECT * FROM {{ ref('stg__order_items') }}
)

SELECT
    oi.order_item_id,
    oi.order_id,

    COALESCE(prod.product_key, MD5('unknown')) AS product_key,
    COALESCE(sell.seller_key, MD5('unknown')) AS seller_key,

    oi.price,
    oi.freight_value

FROM stg_order_items AS oi

LEFT JOIN {{ ref('dim_products') }} AS prod ON oi.product_id = prod.product_id
LEFT JOIN {{ ref('dim_sellers') }} AS sell ON oi.seller_id = sell.seller_id
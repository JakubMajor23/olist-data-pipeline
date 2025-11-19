SELECT
    CAST(order_id AS VARCHAR(50)) AS order_id,
    CAST(order_item_id AS INT) AS order_item_id,
    CAST(product_id AS VARCHAR(50)) AS product_id,
    CAST(seller_id AS VARCHAR(50)) AS seller_id,
    CAST(shipping_limit_date AS TIMESTAMP) AS shipping_limit_date,
    CAST(price AS DOUBLE PRECISION) AS price,
    CAST(freight_value AS DOUBLE PRECISION) AS freight_value
FROM
    {{ source('dwh', 'olist_order_items_dataset') }}
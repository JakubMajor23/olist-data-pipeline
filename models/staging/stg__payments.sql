SELECT
    CAST(order_id AS VARCHAR(50)) AS order_id,
    CAST(payment_sequential AS INT) AS payment_sequential,
    CAST(payment_type AS VARCHAR(20)) AS payment_type,
    CAST(payment_installments AS INT) AS payment_installments,
    CAST(payment_value AS DOUBLE PRECISION) AS payment_value
FROM {{source('dwh', 'olist_order_payments_dataset')}}
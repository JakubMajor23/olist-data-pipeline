WITH stg_orders AS (
    SELECT * FROM {{ ref('stg__orders') }}
),

stg_customers AS (
    SELECT
        customer_id,
        customer_unique_id
    FROM {{ ref('stg__customers') }}
),

stg_order_items AS (
    SELECT * FROM {{ ref('stg__order_items') }}
),

dim_date AS (
    SELECT * FROM {{ ref('dim_date') }}
),

order_totals AS (
    SELECT
        order_id,
        SUM(freight_value) AS total_freight_value,
        SUM(price) AS total_item_value
    FROM stg_order_items
    GROUP BY order_id
)

SELECT
    o.order_id,
    COALESCE(cust.customer_key, MD5('unknown')) AS customer_key,
    COALESCE(stat.order_status_key, MD5('unknown')) AS order_status_key,
    COALESCE(dd_purchase.data_key, 0) AS order_purchase_timestamp_key,
    COALESCE(dd_approved.data_key, 0) AS order_approved_at_key,
    COALESCE(dd_delivered.data_key, 0) AS order_delivered_customer_date_key,
    COALESCE(dd_estimated.data_key, 0) AS order_estimated_delivery_date_key,

    COALESCE(ot.total_freight_value, 0) AS total_freight_value,
    COALESCE(ot.total_item_value, 0) AS total_item_value,
    COALESCE(ot.total_freight_value, 0)
    + COALESCE(ot.total_item_value, 0) AS total_order_value

FROM stg_orders AS o

LEFT JOIN order_totals AS ot ON o.order_id = ot.order_id

LEFT JOIN stg_customers AS sc ON o.customer_id = sc.customer_id
LEFT JOIN
    {{ ref('dim_customers') }} AS cust
    ON sc.customer_unique_id = cust.customer_unique_id
LEFT JOIN
    {{ ref('dim_order_status') }} AS stat
    ON o.order_status = stat.order_status

LEFT JOIN
    dim_date AS dd_purchase
    ON DATE(o.order_purchase_timestamp) = dd_purchase.calendar_date
LEFT JOIN
    dim_date AS dd_approved
    ON DATE(o.order_approved_at) = dd_approved.calendar_date
LEFT JOIN
    dim_date AS dd_delivered
    ON DATE(o.order_delivered_customer_date) = dd_delivered.calendar_date
LEFT JOIN
    dim_date AS dd_estimated
    ON o.order_estimated_delivery_date = dd_estimated.calendar_date

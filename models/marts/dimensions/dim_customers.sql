WITH stg_customers AS (
    SELECT
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state
    FROM
        {{ ref('stg__customers') }}
),

stg_orders AS (
    SELECT
        order_id,
        customer_id,
        order_purchase_timestamp
    FROM
        {{ ref('stg__orders') }}
),

customer_orders_with_date AS (
    SELECT
        c.customer_unique_id,
        c.customer_zip_code_prefix,
        c.customer_city,
        c.customer_state,
        o.order_purchase_timestamp
    FROM
        stg_customers AS c
    LEFT JOIN
        stg_orders AS o ON c.customer_id = o.customer_id
),

deduplicated_customers AS (
    SELECT
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,

        ROW_NUMBER() OVER(
            PARTITION BY customer_unique_id
            ORDER BY order_purchase_timestamp DESC NULLS LAST
        ) AS rn
    FROM
        customer_orders_with_date
)

SELECT
    MD5(dc.customer_unique_id) AS customer_key,

    dc.customer_unique_id,

    dc.customer_city,
    dc.customer_state,

    g.geolocation_key


FROM
    deduplicated_customers AS dc
JOIN {{ref('dim_geolocation')}} AS g
on g.geolocation_zip_code_prefix = dc.customer_zip_code_prefix
WHERE
    dc.rn = 1
WITH stg__payments AS (
    SELECT * FROM "dwh"."raw_data"."stg__payments"
),

stg_orders AS (
    SELECT
        order_id,
        order_purchase_timestamp
    FROM "dwh"."raw_data"."stg__orders"
),

dim_date AS (
    SELECT * FROM "dwh"."raw_data"."dim_date"
)

SELECT
    op.order_id,
    op.payment_value,
    op.payment_installments,
    op.payment_sequential,
    COALESCE(pay_type.payment_type_key, MD5('not_defined')) AS payment_type_key,
    COALESCE(dd_purchase.data_key, 0) AS payment_date_key

FROM stg__payments AS op
LEFT JOIN stg_orders AS o ON op.order_id = o.order_id
LEFT JOIN
    "dwh"."raw_data"."dim_payment_type" AS pay_type
    ON op.payment_type = pay_type.payment_type
LEFT JOIN
    dim_date AS dd_purchase
    ON DATE(o.order_purchase_timestamp) = dd_purchase.calendar_date
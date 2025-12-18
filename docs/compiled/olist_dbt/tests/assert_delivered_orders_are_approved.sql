SELECT
    order_id,
    order_status,
    order_approved_at
FROM
    "dwh"."dwh_main_prod"."stg__orders"
WHERE
    order_status = 'delivered'
    AND order_approved_at IS NULL
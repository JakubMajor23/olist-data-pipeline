WITH fallback_members AS (
    SELECT
        'dim_geolocation' AS model_name,
        EXISTS (
            SELECT 1
            FROM "dwh"."raw_data"."dim_geolocation"
            WHERE geolocation_key = MD5('unknown')
              AND geolocation_zip_code_prefix = 'unknown'
        ) AS member_exists

    UNION ALL

    SELECT
        'dim_products' AS model_name,
        EXISTS (
            SELECT 1
            FROM "dwh"."raw_data"."dim_products"
            WHERE product_key = MD5('unknown')
              AND product_id = 'unknown'
        ) AS member_exists

    UNION ALL

    SELECT
        'dim_customers' AS model_name,
        EXISTS (
            SELECT 1
            FROM "dwh"."raw_data"."dim_customers"
            WHERE customer_key = MD5('unknown')
              AND customer_unique_id = 'unknown'
        ) AS member_exists

    UNION ALL

    SELECT
        'dim_sellers' AS model_name,
        EXISTS (
            SELECT 1
            FROM "dwh"."raw_data"."dim_sellers"
            WHERE seller_key = MD5('unknown')
              AND seller_id = 'unknown'
        ) AS member_exists

    UNION ALL

    SELECT
        'dim_order_status' AS model_name,
        EXISTS (
            SELECT 1
            FROM "dwh"."raw_data"."dim_order_status"
            WHERE order_status_key = MD5('unknown')
              AND order_status = 'unknown'
        ) AS member_exists

    UNION ALL

    SELECT
        'dim_payment_type' AS model_name,
        EXISTS (
            SELECT 1
            FROM "dwh"."raw_data"."dim_payment_type"
            WHERE payment_type_key = MD5('not_defined')
              AND payment_type = 'not_defined'
        ) AS member_exists
)

SELECT
    model_name
FROM
    fallback_members
WHERE
    NOT member_exists
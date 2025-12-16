SELECT
    c.customer_id,
    c.customer_unique_id,
    c.customer_zip_code_prefix,
    TRIM(LOWER(c.customer_city)) AS customer_city,
    TRIM(UPPER(c.customer_state)) AS customer_state
FROM {{ source('dwh', 'olist_customers_dataset') }} AS c

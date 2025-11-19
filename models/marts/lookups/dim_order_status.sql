WITH seed_data AS (
    SELECT * FROM {{ ref('order_statuses') }}
)
SELECT
    CAST(MD5(order_status) AS VARCHAR(32)) AS order_status_key,
    CAST(order_status AS VARCHAR(20)) AS order_status,
    CAST(is_final_status AS BOOLEAN) AS is_final_status
FROM
    seed_data
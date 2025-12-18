WITH seed_data AS (
    SELECT * FROM "dwh"."dwh_main_prod"."payments_types"
)

SELECT
    CAST(MD5(payment_type) AS VARCHAR(32)) AS payment_type_key,
    CAST(payment_type AS VARCHAR(20)) AS payment_type
FROM
    seed_data
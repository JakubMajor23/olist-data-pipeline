SELECT
    CAST(seller_id AS VARCHAR(50)) AS seller_id,
    CAST(seller_zip_code_prefix AS VARCHAR(10)) AS seller_zip_code_prefix,
    CAST(seller_city AS VARCHAR(50)) AS seller_city,
    CAST(seller_state AS VARCHAR(2)) AS seller_state
FROM
    "dwh"."raw_data"."olist_sellers_dataset"
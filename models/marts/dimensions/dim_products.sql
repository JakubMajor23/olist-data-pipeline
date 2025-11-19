SELECT
    CAST(MD5(s.product_id) AS VARCHAR(32)) AS product_key,
    CAST(s.product_id AS VARCHAR(50)) AS product_id,
    CAST(s.product_category_name_english AS VARCHAR(100)) AS product_category_name_english,
    CAST(s.product_name_length AS INT) AS product_name_length,
    CAST(s.product_description_length AS INT) AS product_description_length,
    CAST(s.product_photos_qty AS INT) AS product_photos_qty,
    CAST(s.product_weight_g AS DOUBLE PRECISION) AS product_weight_g,
    CAST(s.product_length_cm AS DOUBLE PRECISION) AS product_length_cm,
    CAST(s.product_height_cm AS DOUBLE PRECISION) AS product_height_cm,
    CAST(s.product_width_cm AS DOUBLE PRECISION) AS product_width_cm

FROM
    {{ ref('stg__products') }} AS s
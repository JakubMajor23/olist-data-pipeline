SELECT
    p.product_id,

    COALESCE(t.product_category_name_english, 'unknown') AS product_category_name_english,
    COALESCE(p.product_name_lenght, 0) AS product_name_length,
    COALESCE(p.product_description_lenght, 0) AS product_description_length,
    COALESCE(p.product_photos_qty, 0) AS product_photos_qty,

    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm

FROM
    {{ source('dwh', 'olist_products_dataset') }} AS p
LEFT JOIN
    {{ ref('product_category_name_translation') }} AS t ON
    p.product_category_name = t.product_category_name

WHERE
    p.product_weight_g IS NOT NULL
    AND p.product_length_cm IS NOT NULL
    AND p.product_height_cm IS NOT NULL
    AND p.product_width_cm IS NOT NULL
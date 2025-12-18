SELECT
    p.product_id,

    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm,

    COALESCE(t.product_category_name_english, 'unknown')
        AS product_category_name_english,
    COALESCE(p.product_name_lenght, 0) AS product_name_length,
    COALESCE(p.product_description_lenght, 0) AS product_description_length,
    COALESCE(p.product_photos_qty, 0) AS product_photos_qty

FROM
    "dwh"."raw_data"."olist_products_dataset" AS p
LEFT JOIN
    "dwh"."dwh_main_prod"."product_category_name_translation" AS t
    ON
        p.product_category_name = t.product_category_name

WHERE
    p.product_weight_g IS NOT NULL
    AND p.product_length_cm IS NOT NULL
    AND p.product_height_cm IS NOT NULL
    AND p.product_width_cm IS NOT NULL
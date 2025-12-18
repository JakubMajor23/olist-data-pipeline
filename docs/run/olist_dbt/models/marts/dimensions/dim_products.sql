
  
    

  create  table "dwh"."dwh_main_prod"."dim_products__dbt_tmp"
  
  
    as
  
  (
    WITH staging AS (
    SELECT
        MD5(s.product_id) AS product_key,
        s.product_id,
        s.product_category_name_english,
        s.product_name_length,
        s.product_description_length,
        s.product_photos_qty,
        s.product_weight_g,
        s.product_length_cm,
        s.product_height_cm,
        s.product_width_cm
    FROM
        "dwh"."dwh_main_prod"."stg__products" AS s
)

SELECT * FROM staging

UNION ALL

SELECT
    MD5('unknown') AS product_key,
    'unknown' AS product_id,
    'unknown' AS product_category_name_english,
    0 AS product_name_length,
    0 AS product_description_length,
    0 AS product_photos_qty,
    0.0 AS product_weight_g,
    0.0 AS product_length_cm,
    0.0 AS product_height_cm,
    0.0 AS product_width_cm
  );
  
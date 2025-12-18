
  
    

  create  table "dwh"."dwh_main_prod"."fact_sales_items__dbt_tmp"
  
  
    as
  
  (
    WITH stg_order_items AS (
    SELECT * FROM "dwh"."dwh_main_prod"."stg__order_items"
)

SELECT
    oi.order_item_id,
    oi.order_id,

    oi.price,
    oi.freight_value,

    COALESCE(prod.product_key, MD5('unknown')) AS product_key,
    COALESCE(sell.seller_key, MD5('unknown')) AS seller_key

FROM stg_order_items AS oi

LEFT JOIN "dwh"."dwh_main_prod"."dim_products" AS prod ON oi.product_id = prod.product_id
LEFT JOIN "dwh"."dwh_main_prod"."dim_sellers" AS sell ON oi.seller_id = sell.seller_id
  );
  
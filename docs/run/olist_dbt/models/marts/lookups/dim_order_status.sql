
  
    

  create  table "dwh"."dwh_main_prod"."dim_order_status__dbt_tmp"
  
  
    as
  
  (
    WITH seed_data AS (
    SELECT * FROM "dwh"."dwh_main_prod"."order_statuses"
)

SELECT
    CAST(MD5(order_status) AS VARCHAR(32)) AS order_status_key,
    CAST(order_status AS VARCHAR(20)) AS order_status,
    CAST(is_final_status AS BOOLEAN) AS is_final_status
FROM
    seed_data
  );
  
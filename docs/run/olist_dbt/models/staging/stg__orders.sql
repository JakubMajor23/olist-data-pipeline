
      
        
        
        delete from "dwh"."dwh_main_prod"."stg__orders" as DBT_INTERNAL_DEST
        where (order_id) in (
            select distinct order_id
            from "stg__orders__dbt_tmp121314921621" as DBT_INTERNAL_SOURCE
        );

    

    insert into "dwh"."dwh_main_prod"."stg__orders" ("order_id", "customer_id", "order_status", "order_approved_at", "is_approval_date_imputed", "order_purchase_timestamp", "order_estimated_delivery_date", "order_delivered_carrier_date", "order_delivered_customer_date")
    (
        select "order_id", "customer_id", "order_status", "order_approved_at", "is_approval_date_imputed", "order_purchase_timestamp", "order_estimated_delivery_date", "order_delivered_carrier_date", "order_delivered_customer_date"
        from "stg__orders__dbt_tmp121314921621"
    )
  
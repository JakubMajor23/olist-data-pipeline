
      
        
        
        delete from "dwh"."dwh_main_prod"."stg__order_items" as DBT_INTERNAL_DEST
        where (order_item_surrogate_key) in (
            select distinct order_item_surrogate_key
            from "stg__order_items__dbt_tmp121314894699" as DBT_INTERNAL_SOURCE
        );

    

    insert into "dwh"."dwh_main_prod"."stg__order_items" ("order_item_surrogate_key", "order_id", "order_item_id", "product_id", "seller_id", "shipping_limit_date", "price", "freight_value", "order_purchase_timestamp")
    (
        select "order_item_surrogate_key", "order_id", "order_item_id", "product_id", "seller_id", "shipping_limit_date", "price", "freight_value", "order_purchase_timestamp"
        from "stg__order_items__dbt_tmp121314894699"
    )
  

      
        
        
        delete from "dwh"."dwh_main_prod"."stg__payments" as DBT_INTERNAL_DEST
        where (payment_id_surrogate) in (
            select distinct payment_id_surrogate
            from "stg__payments__dbt_tmp121315020127" as DBT_INTERNAL_SOURCE
        );

    

    insert into "dwh"."dwh_main_prod"."stg__payments" ("payment_id_surrogate", "order_id", "payment_sequential", "payment_type", "payment_installments", "payment_value", "order_purchase_timestamp")
    (
        select "payment_id_surrogate", "order_id", "payment_sequential", "payment_type", "payment_installments", "payment_value", "order_purchase_timestamp"
        from "stg__payments__dbt_tmp121315020127"
    )
  
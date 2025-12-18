
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select order_estimated_delivery_date_key
from "dwh"."dwh_main_prod"."fact_orders"
where order_estimated_delivery_date_key is null



  
  
      
    ) dbt_internal_test
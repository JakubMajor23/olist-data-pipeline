
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select order_status_key
from "dwh"."dwh_main_prod"."fact_orders"
where order_status_key is null



  
  
      
    ) dbt_internal_test
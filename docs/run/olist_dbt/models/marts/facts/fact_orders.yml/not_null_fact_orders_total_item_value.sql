
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select total_item_value
from "dwh"."dwh_main_prod"."fact_orders"
where total_item_value is null



  
  
      
    ) dbt_internal_test

    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select order_status
from "dwh"."dwh_main_prod"."dim_order_status"
where order_status is null



  
  
      
    ) dbt_internal_test
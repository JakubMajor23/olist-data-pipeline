
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select is_final_status
from "dwh"."dwh_main_prod"."dim_order_status"
where is_final_status is null



  
  
      
    ) dbt_internal_test
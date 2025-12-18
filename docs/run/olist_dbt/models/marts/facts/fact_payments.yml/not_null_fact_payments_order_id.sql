
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select order_id
from "dwh"."dwh_main_prod"."fact_payments"
where order_id is null



  
  
      
    ) dbt_internal_test
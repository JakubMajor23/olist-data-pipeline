
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select customer_state
from "dwh"."dwh_main_prod"."stg__customers"
where customer_state is null



  
  
      
    ) dbt_internal_test
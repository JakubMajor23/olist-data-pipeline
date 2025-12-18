
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select seller_state
from "dwh"."dwh_main_prod"."stg__sellers"
where seller_state is null



  
  
      
    ) dbt_internal_test
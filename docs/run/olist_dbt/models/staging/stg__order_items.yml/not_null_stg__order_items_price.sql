
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select price
from "dwh"."dwh_main_prod"."stg__order_items"
where price is null



  
  
      
    ) dbt_internal_test
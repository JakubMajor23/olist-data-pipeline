
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select shipping_limit_date
from "dwh"."dwh_main_prod"."stg__order_items"
where shipping_limit_date is null



  
  
      
    ) dbt_internal_test
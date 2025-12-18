
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select order_estimated_delivery_date
from "dwh"."dwh_main_prod"."stg__orders"
where order_estimated_delivery_date is null



  
  
      
    ) dbt_internal_test
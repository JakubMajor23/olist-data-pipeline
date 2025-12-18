
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select product_length_cm
from "dwh"."dwh_main_prod"."stg__products"
where product_length_cm is null



  
  
      
    ) dbt_internal_test
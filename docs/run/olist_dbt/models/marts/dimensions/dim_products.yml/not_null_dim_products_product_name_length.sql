
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select product_name_length
from "dwh"."dwh_main_prod"."dim_products"
where product_name_length is null



  
  
      
    ) dbt_internal_test
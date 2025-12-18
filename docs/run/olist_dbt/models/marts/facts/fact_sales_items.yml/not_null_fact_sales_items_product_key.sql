
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select product_key
from "dwh"."dwh_main_prod"."fact_sales_items"
where product_key is null



  
  
      
    ) dbt_internal_test
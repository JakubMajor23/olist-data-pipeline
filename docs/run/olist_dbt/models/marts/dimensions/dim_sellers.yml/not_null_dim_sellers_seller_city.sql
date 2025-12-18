
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select seller_city
from "dwh"."dwh_main_prod"."dim_sellers"
where seller_city is null



  
  
      
    ) dbt_internal_test
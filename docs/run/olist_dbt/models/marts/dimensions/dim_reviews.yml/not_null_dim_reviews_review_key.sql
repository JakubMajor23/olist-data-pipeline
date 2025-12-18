
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select review_key
from "dwh"."dwh_main_prod"."dim_reviews"
where review_key is null



  
  
      
    ) dbt_internal_test
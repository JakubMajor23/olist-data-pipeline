
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select review_score
from "dwh"."dwh_main_prod"."fact_orders"
where review_score is null



  
  
      
    ) dbt_internal_test
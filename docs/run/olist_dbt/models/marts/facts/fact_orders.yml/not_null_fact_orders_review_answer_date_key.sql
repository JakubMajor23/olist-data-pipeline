
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select review_answer_date_key
from "dwh"."dwh_main_prod"."fact_orders"
where review_answer_date_key is null



  
  
      
    ) dbt_internal_test
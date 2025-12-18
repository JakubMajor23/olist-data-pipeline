
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select payment_date_key
from "dwh"."dwh_main_prod"."fact_payments"
where payment_date_key is null



  
  
      
    ) dbt_internal_test
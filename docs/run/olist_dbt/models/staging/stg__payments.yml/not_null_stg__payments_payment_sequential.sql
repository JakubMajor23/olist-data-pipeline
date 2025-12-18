
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select payment_sequential
from "dwh"."dwh_main_prod"."stg__payments"
where payment_sequential is null



  
  
      
    ) dbt_internal_test
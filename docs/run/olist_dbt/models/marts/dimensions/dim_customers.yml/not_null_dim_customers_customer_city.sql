
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select customer_city
from "dwh"."dwh_main_prod"."dim_customers"
where customer_city is null



  
  
      
    ) dbt_internal_test
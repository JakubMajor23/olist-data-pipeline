
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select day_of_week_name
from "dwh"."dwh_main_prod"."dim_date"
where day_of_week_name is null



  
  
      
    ) dbt_internal_test
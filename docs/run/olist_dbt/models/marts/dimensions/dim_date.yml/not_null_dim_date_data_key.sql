
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select data_key
from "dwh"."dwh_main_prod"."dim_date"
where data_key is null



  
  
      
    ) dbt_internal_test
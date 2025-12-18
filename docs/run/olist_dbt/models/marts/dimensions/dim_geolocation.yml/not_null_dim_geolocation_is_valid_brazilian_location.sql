
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select is_valid_brazilian_location
from "dwh"."dwh_main_prod"."dim_geolocation"
where is_valid_brazilian_location is null



  
  
      
    ) dbt_internal_test
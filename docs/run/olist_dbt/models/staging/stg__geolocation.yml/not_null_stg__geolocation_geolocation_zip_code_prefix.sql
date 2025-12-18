
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select geolocation_zip_code_prefix
from "dwh"."dwh_main_prod"."stg__geolocation"
where geolocation_zip_code_prefix is null



  
  
      
    ) dbt_internal_test
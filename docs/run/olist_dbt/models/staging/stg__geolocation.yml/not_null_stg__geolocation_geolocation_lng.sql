
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select geolocation_lng
from (select * from "dwh"."dwh_main_prod"."stg__geolocation" where is_valid_brazilian_location = TRUE) dbt_subquery
where geolocation_lng is null



  
  
      
    ) dbt_internal_test
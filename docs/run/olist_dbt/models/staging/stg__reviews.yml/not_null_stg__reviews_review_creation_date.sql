
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select review_creation_date
from "dwh"."dwh_main_prod"."stg__reviews"
where review_creation_date is null



  
  
      
    ) dbt_internal_test
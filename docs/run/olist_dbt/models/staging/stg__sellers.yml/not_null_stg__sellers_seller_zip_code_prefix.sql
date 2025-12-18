
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select seller_zip_code_prefix
from "dwh"."dwh_main_prod"."stg__sellers"
where seller_zip_code_prefix is null



  
  
      
    ) dbt_internal_test
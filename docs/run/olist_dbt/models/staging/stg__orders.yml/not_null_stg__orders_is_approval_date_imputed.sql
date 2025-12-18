
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select is_approval_date_imputed
from "dwh"."dwh_main_prod"."stg__orders"
where is_approval_date_imputed is null



  
  
      
    ) dbt_internal_test
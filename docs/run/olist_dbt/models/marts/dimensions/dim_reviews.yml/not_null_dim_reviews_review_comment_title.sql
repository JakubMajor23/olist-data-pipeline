
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select review_comment_title
from "dwh"."dwh_main_prod"."dim_reviews"
where review_comment_title is null



  
  
      
    ) dbt_internal_test
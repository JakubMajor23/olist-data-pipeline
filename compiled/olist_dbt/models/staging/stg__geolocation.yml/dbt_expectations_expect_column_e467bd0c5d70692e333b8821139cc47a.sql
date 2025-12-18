






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and geolocation_lng >= -74.0 and geolocation_lng <= -34.0
)
 as expression


    from (select * from "dwh"."raw_data"."stg__geolocation" where is_valid_brazilian_location = TRUE) dbt_subquery
    

),
validation_errors as (

    select
        *
    from
        grouped_expression
    where
        not(expression = true)

)

select *
from validation_errors








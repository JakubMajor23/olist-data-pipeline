






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and day_of_year >= 1 and day_of_year <= 366
)
 as expression


    from "dwh"."raw_data"."dim_date"
    

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















    with grouped_expression as (
    select
        
        
    
  
( 1=1 and day_of_month >= 1 and day_of_month <= 31
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








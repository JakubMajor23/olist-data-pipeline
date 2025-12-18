






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and year >= 2016 and year <= 2018
)
 as expression


    from "dwh"."dwh_main_prod"."dim_date"
    

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








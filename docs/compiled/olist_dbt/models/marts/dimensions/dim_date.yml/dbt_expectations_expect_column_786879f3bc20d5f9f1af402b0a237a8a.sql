






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and day_of_week_num >= 1 and day_of_week_num <= 7
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








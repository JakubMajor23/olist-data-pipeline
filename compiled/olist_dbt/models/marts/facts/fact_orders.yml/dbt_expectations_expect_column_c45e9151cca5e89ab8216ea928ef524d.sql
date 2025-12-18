






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and review_score >= 0 and review_score <= 5
)
 as expression


    from "dwh"."raw_data"."fact_orders"
    

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








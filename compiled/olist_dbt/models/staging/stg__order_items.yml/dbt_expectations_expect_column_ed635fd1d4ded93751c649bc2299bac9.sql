






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and freight_value >= 0
)
 as expression


    from "dwh"."raw_data"."stg__order_items"
    

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








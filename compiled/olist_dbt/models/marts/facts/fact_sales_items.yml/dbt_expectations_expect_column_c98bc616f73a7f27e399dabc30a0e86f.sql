






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and price >= 0
)
 as expression


    from "dwh"."raw_data"."fact_sales_items"
    

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








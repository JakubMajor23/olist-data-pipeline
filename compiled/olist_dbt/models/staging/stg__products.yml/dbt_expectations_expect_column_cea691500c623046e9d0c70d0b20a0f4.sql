






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and product_weight_g >= 0
)
 as expression


    from "dwh"."raw_data"."stg__products"
    

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








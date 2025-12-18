






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and product_photos_qty >= 0
)
 as expression


    from "dwh"."raw_data"."dim_products"
    

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








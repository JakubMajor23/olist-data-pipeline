
    
    

with child as (
    select review_key as from_field
    from "dwh"."dwh_main_prod"."fact_orders"
    where review_key is not null
),

parent as (
    select review_key as to_field
    from "dwh"."dwh_main_prod"."dim_reviews"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



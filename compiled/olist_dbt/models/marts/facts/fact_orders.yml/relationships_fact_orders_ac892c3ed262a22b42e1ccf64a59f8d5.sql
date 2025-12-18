
    
    

with child as (
    select order_status_key as from_field
    from "dwh"."raw_data"."fact_orders"
    where order_status_key is not null
),

parent as (
    select order_status_key as to_field
    from "dwh"."raw_data"."dim_order_status"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



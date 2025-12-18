
    
    

with child as (
    select order_estimated_delivery_date_key as from_field
    from "dwh"."raw_data"."fact_orders"
    where order_estimated_delivery_date_key is not null
),

parent as (
    select data_key as to_field
    from "dwh"."raw_data"."dim_date"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



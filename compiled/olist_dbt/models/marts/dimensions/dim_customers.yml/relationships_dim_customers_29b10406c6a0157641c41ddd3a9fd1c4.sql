
    
    

with child as (
    select geolocation_key as from_field
    from "dwh"."raw_data"."dim_customers"
    where geolocation_key is not null
),

parent as (
    select geolocation_key as to_field
    from "dwh"."raw_data"."dim_geolocation"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



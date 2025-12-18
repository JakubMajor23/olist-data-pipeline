
    
    

with child as (
    select seller_key as from_field
    from "dwh"."raw_data"."fact_sales_items"
    where seller_key is not null
),

parent as (
    select seller_key as to_field
    from "dwh"."raw_data"."dim_sellers"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



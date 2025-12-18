
    
    

with child as (
    select seller_id as from_field
    from "dwh"."dwh_main_prod"."stg__order_items"
    where seller_id is not null
),

parent as (
    select seller_id as to_field
    from "dwh"."dwh_main_prod"."stg__sellers"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



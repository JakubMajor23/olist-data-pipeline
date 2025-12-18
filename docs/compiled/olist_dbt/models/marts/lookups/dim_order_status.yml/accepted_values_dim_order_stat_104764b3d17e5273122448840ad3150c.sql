
    
    

with all_values as (

    select
        order_status as value_field,
        count(*) as n_records

    from "dwh"."dwh_main_prod"."dim_order_status"
    group by order_status

)

select *
from all_values
where value_field not in (
    'delivered','shipped','canceled','invoiced','processing','approved','unavailable','created'
)



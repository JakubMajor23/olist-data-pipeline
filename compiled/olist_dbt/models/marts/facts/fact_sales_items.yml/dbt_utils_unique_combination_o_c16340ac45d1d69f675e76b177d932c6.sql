





with validation_errors as (

    select
        order_id, order_item_id
    from "dwh"."raw_data"."fact_sales_items"
    group by order_id, order_item_id
    having count(*) > 1

)

select *
from validation_errors



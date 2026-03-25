select
    order_id

from "dwh"."main"."stg__orders"

where
    order_status = 'delivered'
    and (
        order_delivered_carrier_date is null
        or order_delivered_customer_date is null
    )
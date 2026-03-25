





with validation_errors as (

    select
        order_id, payment_sequential
    from "dwh"."main"."fact_payments"
    group by order_id, payment_sequential
    having count(*) > 1

)

select *
from validation_errors



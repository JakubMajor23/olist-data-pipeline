
    
    

select
    order_status as unique_field,
    count(*) as n_records

from "dwh"."dwh_main_prod"."dim_order_status"
where order_status is not null
group by order_status
having count(*) > 1




    
    

select
    customer_unique_id as unique_field,
    count(*) as n_records

from "dwh"."dwh_main_prod"."dim_customers"
where customer_unique_id is not null
group by customer_unique_id
having count(*) > 1



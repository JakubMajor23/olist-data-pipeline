
    
    

select
    payment_type as unique_field,
    count(*) as n_records

from "dwh"."main"."dim_payment_type"
where payment_type is not null
group by payment_type
having count(*) > 1



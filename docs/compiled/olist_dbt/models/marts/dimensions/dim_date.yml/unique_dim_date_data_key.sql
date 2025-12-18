
    
    

select
    data_key as unique_field,
    count(*) as n_records

from "dwh"."dwh_main_prod"."dim_date"
where data_key is not null
group by data_key
having count(*) > 1



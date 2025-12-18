
    
    

select
    seller_id as unique_field,
    count(*) as n_records

from "dwh"."dwh_main_prod"."stg__sellers"
where seller_id is not null
group by seller_id
having count(*) > 1



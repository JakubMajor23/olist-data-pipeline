
    
    

with all_values as (

    select
        is_approval_date_imputed as value_field,
        count(*) as n_records

    from "dwh"."raw_data"."stg__orders"
    group by is_approval_date_imputed

)

select *
from all_values
where value_field not in (
    'True','False'
)



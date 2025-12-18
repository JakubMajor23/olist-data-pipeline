
    
    

with all_values as (

    select
        day_of_week_name as value_field,
        count(*) as n_records

    from "dwh"."dwh_main_prod"."dim_date"
    group by day_of_week_name

)

select *
from all_values
where value_field not in (
    'Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday','N/A'
)



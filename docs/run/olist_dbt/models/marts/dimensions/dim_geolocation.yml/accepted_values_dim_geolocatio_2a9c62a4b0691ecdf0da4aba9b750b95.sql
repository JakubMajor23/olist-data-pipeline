
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        is_valid_brazilian_location as value_field,
        count(*) as n_records

    from "dwh"."dwh_main_prod"."dim_geolocation"
    group by is_valid_brazilian_location

)

select *
from all_values
where value_field not in (
    'True','False'
)



  
  
      
    ) dbt_internal_test

    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    geolocation_zip_code_prefix as unique_field,
    count(*) as n_records

from "dwh"."dwh_main_prod"."dim_geolocation"
where geolocation_zip_code_prefix is not null
group by geolocation_zip_code_prefix
having count(*) > 1



  
  
      
    ) dbt_internal_test
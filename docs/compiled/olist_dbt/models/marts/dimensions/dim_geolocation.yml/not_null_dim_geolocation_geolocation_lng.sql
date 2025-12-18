
    
    



select geolocation_lng
from (select * from "dwh"."dwh_main_prod"."dim_geolocation" where is_valid_brazilian_location = TRUE) dbt_subquery
where geolocation_lng is null




    
    



select geolocation_lat
from (select * from "dwh"."main"."dim_geolocation" where is_valid_brazilian_location = TRUE) dbt_subquery
where geolocation_lat is null



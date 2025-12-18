
    
    



select geolocation_lng
from (select * from "dwh"."raw_data"."stg__geolocation" where is_valid_brazilian_location = TRUE) dbt_subquery
where geolocation_lng is null



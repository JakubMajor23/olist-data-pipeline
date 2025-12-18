
    
    



select geolocation_zip_code_prefix
from "dwh"."raw_data"."dim_geolocation"
where geolocation_zip_code_prefix is null



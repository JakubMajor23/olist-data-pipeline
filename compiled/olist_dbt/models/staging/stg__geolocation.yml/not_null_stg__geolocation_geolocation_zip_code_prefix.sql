
    
    



select geolocation_zip_code_prefix
from "dwh"."raw_data"."stg__geolocation"
where geolocation_zip_code_prefix is null



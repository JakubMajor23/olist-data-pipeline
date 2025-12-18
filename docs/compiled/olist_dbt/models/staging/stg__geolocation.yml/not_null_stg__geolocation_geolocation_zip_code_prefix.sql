
    
    



select geolocation_zip_code_prefix
from "dwh"."dwh_main_prod"."stg__geolocation"
where geolocation_zip_code_prefix is null



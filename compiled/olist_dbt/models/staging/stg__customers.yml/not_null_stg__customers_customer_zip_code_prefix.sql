
    
    



select customer_zip_code_prefix
from "dwh"."raw_data"."stg__customers"
where customer_zip_code_prefix is null



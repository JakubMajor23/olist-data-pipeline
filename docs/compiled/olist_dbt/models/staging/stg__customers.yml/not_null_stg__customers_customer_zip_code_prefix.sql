
    
    



select customer_zip_code_prefix
from "dwh"."dwh_main_prod"."stg__customers"
where customer_zip_code_prefix is null



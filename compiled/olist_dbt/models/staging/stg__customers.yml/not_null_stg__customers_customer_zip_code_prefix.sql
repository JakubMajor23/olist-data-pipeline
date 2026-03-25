
    
    



select customer_zip_code_prefix
from "dwh"."main"."stg__customers"
where customer_zip_code_prefix is null



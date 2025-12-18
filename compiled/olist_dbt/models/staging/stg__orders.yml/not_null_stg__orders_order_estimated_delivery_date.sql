
    
    



select order_estimated_delivery_date
from "dwh"."raw_data"."stg__orders"
where order_estimated_delivery_date is null



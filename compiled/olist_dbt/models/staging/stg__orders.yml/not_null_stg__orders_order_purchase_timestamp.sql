
    
    



select order_purchase_timestamp
from "dwh"."raw_data"."stg__orders"
where order_purchase_timestamp is null



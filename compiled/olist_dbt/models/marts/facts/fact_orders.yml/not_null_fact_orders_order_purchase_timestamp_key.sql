
    
    



select order_purchase_timestamp_key
from "dwh"."raw_data"."fact_orders"
where order_purchase_timestamp_key is null



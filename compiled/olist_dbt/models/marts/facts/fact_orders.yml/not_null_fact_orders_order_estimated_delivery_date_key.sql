
    
    



select order_estimated_delivery_date_key
from "dwh"."raw_data"."fact_orders"
where order_estimated_delivery_date_key is null



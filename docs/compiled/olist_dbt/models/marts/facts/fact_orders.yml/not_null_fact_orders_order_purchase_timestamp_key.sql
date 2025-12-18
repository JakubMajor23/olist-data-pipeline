
    
    



select order_purchase_timestamp_key
from "dwh"."dwh_main_prod"."fact_orders"
where order_purchase_timestamp_key is null




    
    



select order_estimated_delivery_date_key
from "dwh"."dwh_main_prod"."fact_orders"
where order_estimated_delivery_date_key is null



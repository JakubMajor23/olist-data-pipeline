
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  select
    order_id

from "dwh"."dwh_main_prod"."stg__orders"

where
    order_status = 'delivered'
    and (
        order_delivered_carrier_date is null
        or order_delivered_customer_date is null
    )
  
  
      
    ) dbt_internal_test
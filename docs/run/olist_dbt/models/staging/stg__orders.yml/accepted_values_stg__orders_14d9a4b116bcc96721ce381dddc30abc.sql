
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        order_status as value_field,
        count(*) as n_records

    from "dwh"."dwh_main_prod"."stg__orders"
    group by order_status

)

select *
from all_values
where value_field not in (
    'delivered','shipped','canceled','invoiced','processing','approved','unavailable','created'
)



  
  
      
    ) dbt_internal_test
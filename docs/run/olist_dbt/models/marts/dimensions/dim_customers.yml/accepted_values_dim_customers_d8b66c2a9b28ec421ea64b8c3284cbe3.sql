
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        customer_state as value_field,
        count(*) as n_records

    from "dwh"."dwh_main_prod"."dim_customers"
    group by customer_state

)

select *
from all_values
where value_field not in (
    'AC','AL','AM','AP','BA','CE','DF','ES','GO','MA','MG','MS','MT','PA','PB','PE','PI','PR','RJ','RN','RO','RR','RS','SC','SE','SP','TO','NA'
)



  
  
      
    ) dbt_internal_test
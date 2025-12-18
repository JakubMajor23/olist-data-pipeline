
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



with validation_errors as (

    select
        order_id,order_item_id,
        count(*) as "n_records"
    from "dwh"."dwh_main_prod"."stg__order_items"
    where
        1=1
        and 
    not (
        order_id is null and 
        order_item_id is null
        
    )


    
    group by
        order_id,order_item_id
    having count(*) > 1

)
select * from validation_errors

  
  
      
    ) dbt_internal_test
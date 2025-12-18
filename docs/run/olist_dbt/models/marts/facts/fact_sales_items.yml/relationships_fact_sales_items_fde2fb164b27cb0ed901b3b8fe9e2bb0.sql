
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select seller_key as from_field
    from "dwh"."dwh_main_prod"."fact_sales_items"
    where seller_key is not null
),

parent as (
    select seller_key as to_field
    from "dwh"."dwh_main_prod"."dim_sellers"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test
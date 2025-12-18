



with validation_errors as (

    select
        order_id,payment_sequential,
        count(*) as "n_records"
    from "dwh"."raw_data"."stg__payments"
    where
        1=1
        and 
    not (
        order_id is null and 
        payment_sequential is null
        
    )


    
    group by
        order_id,payment_sequential
    having count(*) > 1

)
select * from validation_errors

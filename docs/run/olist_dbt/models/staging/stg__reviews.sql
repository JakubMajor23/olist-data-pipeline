
      
        
        
        delete from "dwh"."dwh_main_prod"."stg__reviews" as DBT_INTERNAL_DEST
        where (review_id) in (
            select distinct review_id
            from "stg__reviews__dbt_tmp121315233634" as DBT_INTERNAL_SOURCE
        );

    

    insert into "dwh"."dwh_main_prod"."stg__reviews" ("review_id", "order_id", "review_score", "review_comment_title", "review_comment_message", "review_creation_date", "review_answer_timestamp")
    (
        select "review_id", "order_id", "review_score", "review_comment_title", "review_comment_message", "review_creation_date", "review_answer_timestamp"
        from "stg__reviews__dbt_tmp121315233634"
    )
  
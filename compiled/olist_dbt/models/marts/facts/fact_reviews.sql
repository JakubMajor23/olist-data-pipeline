WITH stg_reviews AS (
    SELECT * FROM "dwh"."raw_data"."stg__reviews"
)

SELECT
    MD5(review_id) AS review_key,
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date,
    review_answer_timestamp
FROM stg_reviews
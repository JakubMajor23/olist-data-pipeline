SELECT
    MD5(review_id) AS review_key,
    review_id,
    review_comment_title,
    review_comment_message
FROM {{ref('stg__reviews')}}
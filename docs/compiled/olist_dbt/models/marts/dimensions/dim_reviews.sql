WITH staging AS (
    SELECT
        MD5(review_id) AS review_key,
        review_id,
        review_comment_title,
        review_comment_message
    FROM "dwh"."dwh_main_prod"."stg__reviews"
)

SELECT * FROM staging

UNION ALL

SELECT
    MD5('unknown') AS review_key,
    'unknown' AS review_id,
    'No Review' AS review_comment_title,
    'No Review' AS review_comment_message
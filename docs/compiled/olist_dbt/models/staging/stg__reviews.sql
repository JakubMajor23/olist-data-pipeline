WITH source AS (
    SELECT *
    FROM "dwh"."raw_data"."olist_order_reviews_dataset"

    
        WHERE
            CAST(review_creation_date AS DATE)
            > (SELECT MAX(review_creation_date) FROM "dwh"."dwh_main_prod"."stg__reviews")
    
),

transformed AS (
    SELECT
        CAST(review_id AS VARCHAR(50)) AS review_id,
        CAST(order_id AS VARCHAR(50)) AS order_id,
        CAST(review_score AS INT) AS review_score,
        COALESCE(CAST(review_comment_title AS VARCHAR(150)), 'No Title')
            AS review_comment_title,
        COALESCE(CAST(review_comment_message AS VARCHAR(500)), 'No Comment')
            AS review_comment_message,
        CAST(review_creation_date AS DATE) AS review_creation_date,
        CAST(review_answer_timestamp AS TIMESTAMP) AS review_answer_timestamp
    FROM source
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY review_id
            ORDER BY order_id
        ) AS rn
    FROM
        transformed
)

SELECT
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date,
    review_answer_timestamp
FROM
    deduplicated
WHERE
    rn = 1
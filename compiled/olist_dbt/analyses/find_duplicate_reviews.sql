SELECT
    review_id,
    COUNT(*) AS review_count
FROM
    "dwh"."main"."stg__reviews"
GROUP BY
    review_id
HAVING
    COUNT(*) > 1
ORDER BY
    review_count DESC;
SELECT
    review_id,
    COUNT(*) AS liczba_wystapien
FROM
    "dwh"."dwh_main_prod"."stg__reviews"
GROUP BY
    review_id
HAVING
    COUNT(*) > 1
ORDER BY
    liczba_wystapien DESC;
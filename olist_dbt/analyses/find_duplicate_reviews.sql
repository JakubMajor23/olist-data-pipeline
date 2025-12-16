SELECT
    review_id,
    COUNT(*) AS liczba_wystapien
FROM
    {{ ref('stg__reviews') }}
GROUP BY
    review_id
HAVING
    COUNT(*) > 1
ORDER BY
    liczba_wystapien DESC;
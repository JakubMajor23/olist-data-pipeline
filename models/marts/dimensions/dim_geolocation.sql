WITH stg_geolocation AS (
    SELECT * FROM {{ ref('stg__geolocation') }}
),

aggregated_coords AS (
    SELECT
        geolocation_zip_code_prefix,
        AVG(geolocation_lat) AS geolocation_lat,
        AVG(geolocation_lng) AS geolocation_lng,

        CASE
            WHEN (AVG(geolocation_lat) BETWEEN -34.0 AND 6.0)
                 AND (AVG(geolocation_lng) BETWEEN -74.0 AND -34.0)
            THEN TRUE
            ELSE FALSE
        END AS is_valid_brazilian_location
    FROM
        stg_geolocation
    GROUP BY
        geolocation_zip_code_prefix
),

ranked_locations AS (
    SELECT
        geolocation_zip_code_prefix,
        geolocation_city,
        geolocation_state,
        ROW_NUMBER() OVER(
            PARTITION BY geolocation_zip_code_prefix
            ORDER BY COUNT(*) DESC
        ) AS rn
    FROM
        stg_geolocation
    GROUP BY
        geolocation_zip_code_prefix,
        geolocation_city,
        geolocation_state
)

SELECT
    MD5(agg.geolocation_zip_code_prefix) AS geolocation_key,
    agg.geolocation_zip_code_prefix,
    agg.geolocation_lat,
    agg.geolocation_lng,
    loc.geolocation_city,
    loc.geolocation_state,

    agg.is_valid_brazilian_location

FROM
    aggregated_coords AS agg
LEFT JOIN
    ranked_locations AS loc
    ON agg.geolocation_zip_code_prefix = loc.geolocation_zip_code_prefix
WHERE
    loc.rn = 1
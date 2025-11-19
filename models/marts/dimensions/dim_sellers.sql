WITH stg_sellers AS (
    SELECT * FROM {{ ref('stg__sellers') }}
),

dim_geolocation AS (
    SELECT * FROM {{ ref('dim_geolocation') }}
)

SELECT
    MD5(s.seller_id) AS seller_key,
    s.seller_id,
    s.seller_zip_code_prefix,
    s.seller_city,
    s.seller_state,

    COALESCE(g.geolocation_key, MD5('00000')) AS geolocation_key

FROM
    stg_sellers AS s
LEFT JOIN
    dim_geolocation AS g
    ON s.seller_zip_code_prefix = g.geolocation_zip_code_prefix
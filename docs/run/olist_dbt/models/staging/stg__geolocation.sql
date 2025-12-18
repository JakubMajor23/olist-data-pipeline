
  
    

  create  table "dwh"."dwh_main_prod"."stg__geolocation__dbt_tmp"
  
  
    as
  
  (
    WITH source AS (
    SELECT * FROM "dwh"."raw_data"."olist_geolocation_dataset"
),

casted_data AS (
    SELECT
        CAST(geolocation_zip_code_prefix AS VARCHAR(10))
            AS geolocation_zip_code_prefix,
        CAST(geolocation_lat AS DOUBLE PRECISION) AS geolocation_lat,
        CAST(geolocation_lng AS DOUBLE PRECISION) AS geolocation_lng,
        CAST(geolocation_city AS VARCHAR(50)) AS geolocation_city,
        CAST(geolocation_state AS VARCHAR(2)) AS geolocation_state
    FROM source
    WHERE
        geolocation_lat IS NOT NULL
        AND geolocation_lng IS NOT NULL
)

SELECT
    geolocation_zip_code_prefix,
    geolocation_lat,
    geolocation_lng,
    geolocation_city,
    geolocation_state,
    COALESCE(
        (geolocation_lat BETWEEN -34.0 AND 6.0)
        AND (geolocation_lng BETWEEN -74.0 AND -34.0),
        FALSE
    ) AS is_valid_brazilian_location
FROM casted_data

UNION ALL

SELECT
    '00000' AS geolocation_zip_code_prefix,
    NULL AS geolocation_lat,
    NULL AS geolocation_lng,
    CAST('unknown' AS VARCHAR(50)) AS geolocation_city,
    CAST('NA' AS VARCHAR(2)) AS geolocation_state,
    FALSE AS is_valid_brazilian_location
  );
  
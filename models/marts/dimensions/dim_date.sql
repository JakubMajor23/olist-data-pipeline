WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2016-01-01' as date)",
        end_date="cast('2018-12-31' as date)"
    ) }}
),


generated_dates AS (
    SELECT
        CAST(date_day AS DATE) AS calendar_date,

        CAST(TO_CHAR(date_day, 'YYYYMMDD') AS INT) AS data_key,

        EXTRACT(YEAR FROM date_day) AS year,
        EXTRACT(QUARTER FROM date_day) AS quarter,
        EXTRACT(MONTH FROM date_day) AS month_num,
        TO_CHAR(date_day, 'YYYY-MM') AS year_month,
        TO_CHAR(date_day, 'FMMonth') AS month_name,
        EXTRACT(DAY FROM date_day) AS day_of_month,
        EXTRACT(DOY FROM date_day) AS day_of_year,
        EXTRACT(ISODOW FROM date_day) AS day_of_week_num,
        TO_CHAR(date_day, 'FMDay') AS day_of_week_name,
        CASE
            WHEN EXTRACT(ISODOW FROM date_day) IN (6, 7) THEN TRUE
            ELSE FALSE
        END AS is_weekend

    FROM date_spine
),

-- special row for null values
special_row AS (
    SELECT
        CAST(NULL AS DATE) AS calendar_date,
        0 AS data_key,
        CAST(NULL AS INT) AS year,
        CAST(NULL AS INT) AS quarter,
        CAST(NULL AS INT) AS month_num,
        'N/A' AS year_month,
        'N/A' AS month_name,
        CAST(NULL AS INT) AS day_of_month,
        CAST(NULL AS INT) AS day_of_year,
        CAST(NULL AS INT) AS day_of_week_num,
        'N/A' AS day_of_week_name,
        FALSE AS is_weekend
)


SELECT * FROM generated_dates
UNION ALL
SELECT * FROM special_row
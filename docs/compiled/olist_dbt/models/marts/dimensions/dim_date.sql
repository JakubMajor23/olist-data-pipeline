WITH date_spine AS (
    





with rawdata as (

    

    

    with p as (
        select 0 as generated_number union all select 1
    ), unioned as (

    select

    
    p0.generated_number * power(2, 0)
     + 
    
    p1.generated_number * power(2, 1)
     + 
    
    p2.generated_number * power(2, 2)
     + 
    
    p3.generated_number * power(2, 3)
     + 
    
    p4.generated_number * power(2, 4)
     + 
    
    p5.generated_number * power(2, 5)
     + 
    
    p6.generated_number * power(2, 6)
     + 
    
    p7.generated_number * power(2, 7)
     + 
    
    p8.generated_number * power(2, 8)
     + 
    
    p9.generated_number * power(2, 9)
     + 
    
    p10.generated_number * power(2, 10)
    
    
    + 1
    as generated_number

    from

    
    p as p0
     cross join 
    
    p as p1
     cross join 
    
    p as p2
     cross join 
    
    p as p3
     cross join 
    
    p as p4
     cross join 
    
    p as p5
     cross join 
    
    p as p6
     cross join 
    
    p as p7
     cross join 
    
    p as p8
     cross join 
    
    p as p9
     cross join 
    
    p as p10
    
    

    )

    select *
    from unioned
    where generated_number <= 1095
    order by generated_number



),

all_periods as (

    select (
        

    cast('2016-01-01' as date) + ((interval '1 day') * (row_number() over (order by 1) - 1))


    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= cast('2018-12-31' as date)

)

select * from filtered


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
        COALESCE(EXTRACT(ISODOW FROM date_day) IN (6, 7), FALSE) AS is_weekend

    FROM date_spine
),

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
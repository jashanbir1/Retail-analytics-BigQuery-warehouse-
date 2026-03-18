{{ config(
    materialized='table',
    schema='retail_gold'
) }}

WITH date_spine AS (
    SELECT
        day AS date_day
    FROM UNNEST(
        GENERATE_DATE_ARRAY('2026-01-01', '2026-12-31')
    ) AS day
)

SELECT 
    date_day,
    extract(year from date_day) as year_number,
    extract(month from date_day) as month_number,
    extract(day from date_day) as day_number,
    format_date('%B', date_day) as month_name,
    format_date('%A', date_day) as day_name,
    extract(dayofweek from date_day) as day_of_week,
    extract(quarter from date_day) as quarter_number,
    CASE
        WHEN extract(dayofweek from date_day) IN (1,7) THEN TRUE
        ELSE FALSE
    END AS is_weekend

FROM date_spine
ORDER BY date_day



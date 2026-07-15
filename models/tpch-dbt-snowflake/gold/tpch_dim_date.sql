{{ config(materialized='table', schema='tpch_gold') }}

with spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('1992-01-01' as date)",
        end_date="cast('1998-12-31' as date)"
    ) }}
)
select
    date_day               as date_key,
    year(date_day)          as year,
    quarter(date_day)       as quarter,
    month(date_day)         as month,
    monthname(date_day)     as month_name,
    day(date_day)           as day,
    dayname(date_day)       as day_name,
    weekofyear(date_day)    as week_of_year
from spine
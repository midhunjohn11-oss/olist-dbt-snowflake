{{ config(
    materialized='table'
) }}

WITH date_spine AS (
    SELECT DATEADD('day', SEQ4(), '2016-01-01'::DATE) AS DATE_DAY
    FROM TABLE(GENERATOR(ROWCOUNT => 1461))
),

final AS (
    SELECT
        DATE_DAY                                        AS DATE_ID,
        DATE_PART('year',    DATE_DAY)                  AS YEAR,
        DATE_PART('month',   DATE_DAY)                  AS MONTH_NUM,
        MONTHNAME(DATE_DAY)                             AS MONTH_NAME,
        DATE_PART('quarter', DATE_DAY)                  AS QUARTER,
        DATE_PART('week',    DATE_DAY)                  AS WEEK_NUM,
        DATE_PART('dayofweek', DATE_DAY)                AS DAY_OF_WEEK,
        DAYNAME(DATE_DAY)                               AS DAY_NAME,
        CASE
            WHEN DATE_PART('dayofweek', DATE_DAY)
                IN (0, 6) THEN TRUE ELSE FALSE
        END                                             AS IS_WEEKEND,
        DATE_TRUNC('month',   DATE_DAY)                 AS FIRST_DAY_OF_MONTH,
        DATE_TRUNC('quarter', DATE_DAY)                 AS FIRST_DAY_OF_QUARTER
    FROM date_spine
)

SELECT * FROM final
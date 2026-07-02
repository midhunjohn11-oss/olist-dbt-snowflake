{{ config(
    materialized='table'
) }}

WITH base AS (
    SELECT
        f.ORDER_YEAR,
        f.ORDER_MONTH_NUM,
        d.MONTH_NAME,
        c.CUSTOMER_STATE,
        c.CUSTOMER_REGION,
        p.PRODUCT_CATEGORY_NAME,
        COUNT(DISTINCT f.ORDER_ID)                      AS TOTAL_ORDERS,
        COUNT(DISTINCT f.CUSTOMER_ID)                   AS UNIQUE_CUSTOMERS,
        SUM(f.TOTAL_PAYMENT_VALUE)                      AS TOTAL_REVENUE,
        AVG(f.TOTAL_PAYMENT_VALUE)                      AS AVG_ORDER_VALUE,
        SUM(f.TOTAL_FREIGHT_VALUE)                      AS TOTAL_FREIGHT,
        ROUND(
            SUM(f.TOTAL_FREIGHT_VALUE)
            / NULLIF(SUM(f.TOTAL_PAYMENT_VALUE), 0) * 100
        , 2)                                            AS FREIGHT_PCT_OF_REVENUE,
        AVG(f.ACTUAL_DELIVERY_DAYS)                     AS AVG_DELIVERY_DAYS,
        SUM(CASE WHEN f.IS_LATE_DELIVERY
            THEN 1 ELSE 0 END)                          AS LATE_DELIVERIES,
        ROUND(
            SUM(CASE WHEN f.IS_LATE_DELIVERY
                THEN 1 ELSE 0 END)
            / NULLIF(COUNT(*), 0) * 100
        , 2)                                            AS LATE_DELIVERY_PCT
    FROM {{ ref('fct_orders') }}            f
    LEFT JOIN {{ ref('dim_customer') }}     c  ON f.CUSTOMER_ID  = c.CUSTOMER_ID
    LEFT JOIN {{ ref('stg_order_items') }}  oi ON f.ORDER_ID     = oi.ORDER_ID
    LEFT JOIN {{ ref('dim_product') }}      p  ON oi.PRODUCT_ID  = p.PRODUCT_ID
    LEFT JOIN {{ ref('dim_date') }}         d  ON f.ORDER_DATE   = d.DATE_ID
    WHERE f.ORDER_STATUS = 'DELIVERED'
    GROUP BY 1,2,3,4,5,6
)

SELECT * FROM base
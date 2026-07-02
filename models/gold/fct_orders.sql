{{ config(
    materialized='incremental',
    unique_key='ORDER_ID',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    schema='GOLD'
) }}

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
    {% if is_incremental() %}
        WHERE _LOADED_AT > (SELECT MAX(_LOADED_AT) FROM {{ this }})
    {% endif %}
),

payments AS (
    SELECT
        ORDER_ID,
        SUM(PAYMENT_VALUE)                               AS TOTAL_PAYMENT_VALUE,
        MAX(PAYMENT_INSTALLMENTS)                        AS MAX_INSTALLMENTS,
        COUNT(DISTINCT PAYMENT_TYPE)                     AS PAYMENT_TYPE_COUNT,
        MAX(CASE WHEN IS_CREDIT_CARD THEN 1 ELSE 0 END) AS HAS_CREDIT_CARD
    FROM {{ ref('stg_payments') }}
    GROUP BY ORDER_ID
),

items AS (
    SELECT
        ORDER_ID,
        COUNT(*)                                         AS ITEM_COUNT,
        SUM(PRICE)                                       AS TOTAL_ITEMS_PRICE,
        SUM(FREIGHT_VALUE)                               AS TOTAL_FREIGHT_VALUE,
        SUM(TOTAL_ITEM_VALUE)                            AS TOTAL_ORDER_VALUE,
        COUNT(DISTINCT PRODUCT_ID)                       AS DISTINCT_PRODUCTS,
        COUNT(DISTINCT SELLER_ID)                        AS DISTINCT_SELLERS
    FROM {{ ref('stg_order_items') }}
    GROUP BY ORDER_ID
)

SELECT
    -- Order keys
    o.ORDER_ID,
    o.CUSTOMER_ID,

    -- Order status and timestamps
    o.ORDER_STATUS,
    o.ORDER_PURCHASE_TIMESTAMP,
    o.ORDER_APPROVED_AT,
    o.ORDER_DELIVERED_CARRIER_DATE,
    o.ORDER_DELIVERED_CUSTOMER_DATE,
    o.ORDER_ESTIMATED_DELIVERY_DATE,

    -- Delivery metrics
    o.ACTUAL_DELIVERY_DAYS,
    o.ESTIMATED_DELIVERY_DAYS,
    o.IS_LATE_DELIVERY,

    -- Item metrics (COALESCE handles orders with no items)
    COALESCE(i.ITEM_COUNT,         0)                    AS ITEM_COUNT,
    COALESCE(i.TOTAL_ITEMS_PRICE,  0)                    AS TOTAL_ITEMS_PRICE,
    COALESCE(i.TOTAL_FREIGHT_VALUE,0)                    AS TOTAL_FREIGHT_VALUE,
    COALESCE(i.TOTAL_ORDER_VALUE,  0)                    AS TOTAL_ORDER_VALUE,
    COALESCE(i.DISTINCT_PRODUCTS,  0)                    AS DISTINCT_PRODUCTS,
    COALESCE(i.DISTINCT_SELLERS,   0)                    AS DISTINCT_SELLERS,

    -- Payment metrics (COALESCE handles orders with no payments)
    COALESCE(p.TOTAL_PAYMENT_VALUE,0)                    AS TOTAL_PAYMENT_VALUE,
    COALESCE(p.MAX_INSTALLMENTS,   0)                    AS MAX_INSTALLMENTS,
    COALESCE(p.PAYMENT_TYPE_COUNT, 0)                    AS PAYMENT_TYPE_COUNT,
    COALESCE(p.HAS_CREDIT_CARD,    0)                    AS HAS_CREDIT_CARD,

    -- Date dimension keys
    TO_DATE(o.ORDER_PURCHASE_TIMESTAMP)                  AS ORDER_DATE,
    DATE_TRUNC('month', o.ORDER_PURCHASE_TIMESTAMP)      AS ORDER_MONTH,
    DATE_PART('year',   o.ORDER_PURCHASE_TIMESTAMP)      AS ORDER_YEAR,
    DATE_PART('month',  o.ORDER_PURCHASE_TIMESTAMP)      AS ORDER_MONTH_NUM,

    -- Metadata
    o._LOADED_AT

FROM orders        o
LEFT JOIN items    i ON o.ORDER_ID = i.ORDER_ID
LEFT JOIN payments p ON o.ORDER_ID = p.ORDER_ID
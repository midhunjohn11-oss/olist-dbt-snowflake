{% snapshot scd_fct_orders %}

{{
    config(
        target_schema='GOLD',
        unique_key='ORDER_ID',
        strategy='check',
        check_cols=['ORDER_STATUS',
                    'ORDER_APPROVED_AT',
                    'ORDER_DELIVERED_CARRIER_DATE',
                    'ORDER_DELIVERED_CUSTOMER_DATE'],
        invalidate_hard_deletes=True
    )
}}

SELECT
    ORDER_ID,
    CUSTOMER_ID,
    ORDER_STATUS,
    ORDER_PURCHASE_TIMESTAMP,
    ORDER_APPROVED_AT,
    ORDER_DELIVERED_CARRIER_DATE,
    ORDER_DELIVERED_CUSTOMER_DATE,
    ORDER_ESTIMATED_DELIVERY_DATE,
    ACTUAL_DELIVERY_DAYS,
    IS_LATE_DELIVERY,
    TOTAL_PAYMENT_VALUE,
    TOTAL_ORDER_VALUE,
    ORDER_DATE,
    ORDER_YEAR,
    ORDER_MONTH_NUM,
    _LOADED_AT
FROM {{ ref('fct_orders') }}

{% endsnapshot %}
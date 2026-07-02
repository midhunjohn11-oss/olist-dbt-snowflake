{{ config(
    materialized='table',
    schema='GOLD'
) }}

SELECT
    CUSTOMER_ID,
    CUSTOMER_UNIQUE_ID,
    CUSTOMER_ZIP_CODE_PREFIX,
    CUSTOMER_CITY,
    CUSTOMER_STATE,
    CUSTOMER_REGION,
    _LOADED_AT
FROM {{ ref('stg_customers') }}
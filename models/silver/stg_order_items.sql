{{ config(
    materialized='incremental',
    unique_key=['ORDER_ID', 'ORDER_ITEM_ID'],
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}

WITH source AS (
    SELECT * FROM {{ source('bronze', 'RAW_ORDER_ITEMS') }}
    {% if is_incremental() %}
        WHERE _LOADED_AT > (SELECT MAX(_LOADED_AT) FROM {{ this }})
    {% endif %}
),

deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY ORDER_ID, ORDER_ITEM_ID
            ORDER BY _LOADED_AT DESC
        ) AS row_num
    FROM source
),

cleaned AS (
    SELECT
        ORDER_ID,
        TRY_TO_NUMBER(ORDER_ITEM_ID)                    AS ORDER_ITEM_ID,
        PRODUCT_ID,
        SELLER_ID,
        TRY_TO_TIMESTAMP_NTZ(SHIPPING_LIMIT_DATE)       AS SHIPPING_LIMIT_DATE,
        TRY_TO_NUMBER(PRICE, 10, 2)                     AS PRICE,
        TRY_TO_NUMBER(FREIGHT_VALUE, 10, 2)             AS FREIGHT_VALUE,
        -- Derived
        TRY_TO_NUMBER(PRICE, 10, 2)
            + TRY_TO_NUMBER(FREIGHT_VALUE, 10, 2)       AS TOTAL_ITEM_VALUE,
        -- Metadata
        _SOURCE_FILE,
        _SOURCE_SYSTEM,
        _LOADED_AT,
        _LOAD_DATE,
        _BATCH_ID
    FROM deduped
    WHERE row_num = 1
      AND ORDER_ID IS NOT NULL
      AND ORDER_ITEM_ID IS NOT NULL
)

SELECT * FROM cleaned
{{ config(
    materialized='incremental',
    unique_key='PRODUCT_ID',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}

WITH source AS (
    SELECT * FROM {{ source('bronze', 'RAW_PRODUCTS') }}
    {% if is_incremental() %}
        WHERE _LOADED_AT > (SELECT MAX(_LOADED_AT) FROM {{ this }})
    {% endif %}
),

deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY PRODUCT_ID
            ORDER BY _LOADED_AT DESC
        ) AS row_num
    FROM source
),

cleaned AS (
    SELECT
        PRODUCT_ID,
        -- Fix source typos in column names
        LOWER(TRIM(PRODUCT_CATEGORY_NAME))              AS PRODUCT_CATEGORY_NAME,
        TRY_TO_NUMBER(PRODUCT_NAME_LENGHT)              AS PRODUCT_NAME_LENGTH,
        TRY_TO_NUMBER(PRODUCT_DESCRIPTION_LENGHT)       AS PRODUCT_DESCRIPTION_LENGTH,
        TRY_TO_NUMBER(PRODUCT_PHOTOS_QTY)               AS PRODUCT_PHOTOS_QTY,
        TRY_TO_NUMBER(PRODUCT_WEIGHT_G)                 AS PRODUCT_WEIGHT_G,
        TRY_TO_NUMBER(PRODUCT_LENGTH_CM)                AS PRODUCT_LENGTH_CM,
        TRY_TO_NUMBER(PRODUCT_HEIGHT_CM)                AS PRODUCT_HEIGHT_CM,
        TRY_TO_NUMBER(PRODUCT_WIDTH_CM)                 AS PRODUCT_WIDTH_CM,
        -- Derived
        CASE
            WHEN TRY_TO_NUMBER(PRODUCT_WEIGHT_G) < 500   THEN 'Light'
            WHEN TRY_TO_NUMBER(PRODUCT_WEIGHT_G) < 2000  THEN 'Medium'
            WHEN TRY_TO_NUMBER(PRODUCT_WEIGHT_G) < 5000  THEN 'Heavy'
            ELSE 'Very Heavy'
        END                                             AS WEIGHT_CLASS,
        -- Metadata
        _SOURCE_FILE,
        _SOURCE_SYSTEM,
        _LOADED_AT,
        _LOAD_DATE,
        _BATCH_ID
    FROM deduped
    WHERE row_num = 1
      AND PRODUCT_ID IS NOT NULL
)

SELECT * FROM cleaned
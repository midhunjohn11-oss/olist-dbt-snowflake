{{ config(
    materialized='incremental',
    unique_key='CUSTOMER_ID',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}

WITH source AS (
    SELECT * FROM {{ source('bronze', 'RAW_CUSTOMERS') }}
    {% if is_incremental() %}
        WHERE _LOADED_AT > (SELECT MAX(_LOADED_AT) FROM {{ this }})
    {% endif %}
),

deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY CUSTOMER_ID
            ORDER BY _LOADED_AT DESC
        ) AS row_num
    FROM source
),

cleaned AS (
    SELECT
        CUSTOMER_ID,
        CUSTOMER_UNIQUE_ID,
        CUSTOMER_ZIP_CODE_PREFIX,
        INITCAP(TRIM(CUSTOMER_CITY))                    AS CUSTOMER_CITY,
        UPPER(TRIM(CUSTOMER_STATE))                     AS CUSTOMER_STATE,
        -- Derived — Brazilian regions
        CASE UPPER(TRIM(CUSTOMER_STATE))
            WHEN 'SP' THEN 'Southeast'
            WHEN 'RJ' THEN 'Southeast'
            WHEN 'MG' THEN 'Southeast'
            WHEN 'ES' THEN 'Southeast'
            WHEN 'PR' THEN 'South'
            WHEN 'SC' THEN 'South'
            WHEN 'RS' THEN 'South'
            WHEN 'MT' THEN 'Centre-West'
            WHEN 'MS' THEN 'Centre-West'
            WHEN 'GO' THEN 'Centre-West'
            WHEN 'DF' THEN 'Centre-West'
            WHEN 'BA' THEN 'Northeast'
            WHEN 'CE' THEN 'Northeast'
            WHEN 'MA' THEN 'Northeast'
            WHEN 'PB' THEN 'Northeast'
            WHEN 'PE' THEN 'Northeast'
            WHEN 'PI' THEN 'Northeast'
            WHEN 'RN' THEN 'Northeast'
            WHEN 'SE' THEN 'Northeast'
            WHEN 'AL' THEN 'Northeast'
            WHEN 'AM' THEN 'North'
            WHEN 'PA' THEN 'North'
            WHEN 'RO' THEN 'North'
            WHEN 'RR' THEN 'North'
            WHEN 'AC' THEN 'North'
            WHEN 'AP' THEN 'North'
            WHEN 'TO' THEN 'North'
            ELSE 'Unknown'
        END                                             AS CUSTOMER_REGION,
        -- Metadata
        _SOURCE_FILE,
        _SOURCE_SYSTEM,
        _LOADED_AT,
        _LOAD_DATE,
        _BATCH_ID
    FROM deduped
    WHERE row_num = 1
      AND CUSTOMER_ID IS NOT NULL
)

SELECT * FROM cleaned
{{ config(
    materialized='incremental',
    unique_key='ORDER_ID',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}
WITH source AS (
    SELECT * FROM {{ source('bronze', 'RAW_ORDERS') }}

    {% if is_incremental() %}
        -- On incremental runs, only process rows loaded since last run
        WHERE _LOADED_AT > (SELECT MAX(_LOADED_AT) FROM {{ this }})
    {% endif %}
),

deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY ORDER_ID
            ORDER BY _LOADED_AT DESC
        ) AS row_num
    FROM source
),

cleaned AS (
    SELECT
        ORDER_ID,
        CUSTOMER_ID,
        UPPER(TRIM(ORDER_STATUS))                            AS ORDER_STATUS,
        TRY_TO_TIMESTAMP_NTZ(ORDER_PURCHASE_TIMESTAMP)      AS ORDER_PURCHASE_TIMESTAMP,
        TRY_TO_TIMESTAMP_NTZ(ORDER_APPROVED_AT)             AS ORDER_APPROVED_AT,
        TRY_TO_TIMESTAMP_NTZ(ORDER_DELIVERED_CARRIER_DATE)  AS ORDER_DELIVERED_CARRIER_DATE,
        TRY_TO_TIMESTAMP_NTZ(ORDER_DELIVERED_CUSTOMER_DATE) AS ORDER_DELIVERED_CUSTOMER_DATE,
        TRY_TO_TIMESTAMP_NTZ(ORDER_ESTIMATED_DELIVERY_DATE) AS ORDER_ESTIMATED_DELIVERY_DATE,
        DATEDIFF('day',
            TRY_TO_TIMESTAMP_NTZ(ORDER_PURCHASE_TIMESTAMP),
            TRY_TO_TIMESTAMP_NTZ(ORDER_DELIVERED_CUSTOMER_DATE)
        )                                                    AS ACTUAL_DELIVERY_DAYS,
        DATEDIFF('day',
            TRY_TO_TIMESTAMP_NTZ(ORDER_PURCHASE_TIMESTAMP),
            TRY_TO_TIMESTAMP_NTZ(ORDER_ESTIMATED_DELIVERY_DATE)
        )                                                    AS ESTIMATED_DELIVERY_DAYS,
        CASE
            WHEN TRY_TO_TIMESTAMP_NTZ(ORDER_DELIVERED_CUSTOMER_DATE)
                 > TRY_TO_TIMESTAMP_NTZ(ORDER_ESTIMATED_DELIVERY_DATE)
            THEN TRUE ELSE FALSE
        END                                                  AS IS_LATE_DELIVERY,
        _SOURCE_FILE,
        _SOURCE_SYSTEM,
        _LOADED_AT,
        _LOAD_DATE,
        _BATCH_ID
    FROM deduped
    WHERE row_num = 1
      AND ORDER_ID IS NOT NULL
)

SELECT * FROM cleaned
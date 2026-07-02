{{ config(
    materialized='incremental',
    unique_key=['ORDER_ID', 'PAYMENT_SEQUENTIAL'],
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}

WITH source AS (
    SELECT * FROM {{ source('bronze', 'RAW_PAYMENTS') }}
    {% if is_incremental() %}
        WHERE _LOADED_AT > (SELECT MAX(_LOADED_AT) FROM {{ this }})
    {% endif %}
),

deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY ORDER_ID, PAYMENT_SEQUENTIAL
            ORDER BY _LOADED_AT DESC
        ) AS row_num
    FROM source
),

cleaned AS (
    SELECT
        ORDER_ID,
        TRY_TO_NUMBER(PAYMENT_SEQUENTIAL)               AS PAYMENT_SEQUENTIAL,
        UPPER(TRIM(PAYMENT_TYPE))                       AS PAYMENT_TYPE,
        TRY_TO_NUMBER(PAYMENT_INSTALLMENTS)             AS PAYMENT_INSTALLMENTS,
        TRY_TO_NUMBER(PAYMENT_VALUE, 10, 2)             AS PAYMENT_VALUE,
        -- Derived
        CASE
            WHEN UPPER(TRIM(PAYMENT_TYPE)) = 'CREDIT_CARD' THEN TRUE
            ELSE FALSE
        END                                             AS IS_CREDIT_CARD,
        CASE
            WHEN TRY_TO_NUMBER(PAYMENT_INSTALLMENTS) > 1 THEN TRUE
            ELSE FALSE
        END                                             AS IS_INSTALLMENT,
        -- Metadata
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
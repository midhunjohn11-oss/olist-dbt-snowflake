{{ config(
    materialized='table'
) }}

SELECT
    PRODUCT_ID,
    PRODUCT_CATEGORY_NAME,
    PRODUCT_PHOTOS_QTY,
    PRODUCT_WEIGHT_G,
    PRODUCT_LENGTH_CM,
    PRODUCT_HEIGHT_CM,
    PRODUCT_WIDTH_CM,
    WEIGHT_CLASS,
    _LOADED_AT
FROM {{ ref('stg_products') }}
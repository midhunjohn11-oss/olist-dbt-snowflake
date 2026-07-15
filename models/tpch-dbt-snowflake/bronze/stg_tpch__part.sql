{{ config(materialized='incremental', incremental_strategy='append', schema='tpch_bronze') }}

select
    p_partkey     as part_key,
    p_name        as part_name,
    p_mfgr        as manufacturer,
    p_brand       as brand,
    p_type        as part_type,
    p_size        as part_size,
    p_container   as container,
    p_retailprice as retail_price,
    current_timestamp() as _loaded_at
from {{ source('tpch_sample', 'part') }} src
{% if is_incremental() %}
where not exists (
    select 1 from {{ this }} tgt where tgt.part_key = src.p_partkey
)
{% endif %}
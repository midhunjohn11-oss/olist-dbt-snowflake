{{ config(materialized='incremental', incremental_strategy='append', schema='tpch_bronze') }}

select r_regionkey as region_key, r_name as region_name,
       current_timestamp() as _loaded_at
from {{ source('tpch_sample', 'region') }} src
{% if is_incremental() %}
where not exists (select 1 from {{ this }} tgt where tgt.region_key = src.r_regionkey)
{% endif %}
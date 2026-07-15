{{ config(materialized='incremental', incremental_strategy='append', schema='tpch_bronze') }}

select n_nationkey as nation_key, n_name as nation_name, n_regionkey as region_key,
       current_timestamp() as _loaded_at
from {{ source('tpch_sample', 'nation') }} src
{% if is_incremental() %}
where not exists (select 1 from {{ this }} tgt where tgt.nation_key = src.n_nationkey)
{% endif %}
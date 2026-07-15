{{ config(materialized='incremental', incremental_strategy='append', schema='tpch_bronze') }}

select
    ps_partkey    as part_key,
    ps_suppkey    as supplier_key,
    ps_availqty   as available_qty,
    ps_supplycost as supply_cost,
    current_timestamp() as _loaded_at
from {{ source('tpch_sample', 'partsupp') }} src
{% if is_incremental() %}
where not exists (
    select 1 from {{ this }} tgt
    where tgt.part_key = src.ps_partkey
      and tgt.supplier_key = src.ps_suppkey
)
{% endif %}
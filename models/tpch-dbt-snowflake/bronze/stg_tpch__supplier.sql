{{ config(materialized='incremental', incremental_strategy='append', schema='tpch_bronze') }}

select
    s_suppkey   as supplier_key,
    s_name      as supplier_name,
    s_address   as supplier_address,
    s_nationkey as nation_key,
    s_phone     as supplier_phone,
    s_acctbal   as account_balance,
    current_timestamp() as _loaded_at
from {{ source('tpch_sample', 'supplier') }} src
{% if is_incremental() %}
where not exists (
    select 1 from {{ this }} tgt where tgt.supplier_key = src.s_suppkey
)
{% endif %}
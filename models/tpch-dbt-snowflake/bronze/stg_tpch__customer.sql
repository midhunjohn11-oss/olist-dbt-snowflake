{{ config(materialized='incremental', incremental_strategy='append', schema='tpch_bronze') }}

select
    c_custkey    as customer_key,
    c_name       as customer_name,
    c_address    as customer_address,
    c_nationkey  as nation_key,
    c_phone      as customer_phone,
    c_acctbal    as account_balance,
    c_mktsegment as market_segment,
    c_comment    as customer_comment,
    current_timestamp() as _loaded_at
from {{ source('tpch_sample', 'customer') }} src
{% if is_incremental() %}
where not exists (
    select 1 from {{ this }} tgt where tgt.customer_key = src.c_custkey
)
{% endif %}
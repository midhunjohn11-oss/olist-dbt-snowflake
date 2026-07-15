{{ config(materialized='incremental', incremental_strategy='append', schema='tpch_bronze') }}

select
    o_orderkey      as order_key,
    o_custkey       as customer_key,
    o_orderstatus   as order_status,
    o_totalprice    as total_price,
    o_orderdate     as order_date,
    o_orderpriority as order_priority,
    o_clerk         as clerk,
    o_shippriority  as ship_priority,
    current_timestamp() as _loaded_at
from {{ source('tpch_sample', 'orders') }} src
{% if is_incremental() %}
where not exists (
    select 1 from {{ this }} tgt where tgt.order_key = src.o_orderkey
)
{% endif %}
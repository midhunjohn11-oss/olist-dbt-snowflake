{{ config(materialized='incremental', incremental_strategy='append', schema='tpch_bronze') }}

select
    l_orderkey      as order_key,
    l_partkey       as part_key,
    l_suppkey       as supplier_key,
    l_linenumber    as line_number,
    l_quantity      as quantity,
    l_extendedprice as extended_price,
    l_discount      as discount,
    l_tax           as tax,
    l_returnflag    as return_flag,
    l_linestatus    as line_status,
    l_shipdate      as ship_date,
    l_commitdate    as commit_date,
    l_receiptdate   as receipt_date,
    l_shipinstruct  as ship_instructions,
    l_shipmode      as ship_mode,
    current_timestamp() as _loaded_at
from {{ source('tpch_sample', 'lineitem') }} src
{% if is_incremental() %}
where not exists (
    select 1 from {{ this }} tgt
    where tgt.order_key = src.l_orderkey
      and tgt.line_number = src.l_linenumber
)
{% endif %}
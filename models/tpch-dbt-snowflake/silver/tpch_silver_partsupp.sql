{{ config(materialized='incremental', incremental_strategy='merge', unique_key=['part_key','supplier_key'], schema='tpch_silver') }}

with source as (
    select * from {{ ref('stg_tpch__partsupp') }}
    {% if is_incremental() %}
    where _loaded_at > (select coalesce(max(_loaded_at), '1900-01-01'::timestamp) from {{ this }})
    {% endif %}
),

deduped as (
    select
        part_key, supplier_key, available_qty, supply_cost,
        _loaded_at,
        row_number() over (partition by part_key, supplier_key order by _loaded_at desc) as rn
    from source
    where available_qty >= 0
      and supply_cost >= 0
)

select part_key, supplier_key, available_qty, supply_cost, _loaded_at
from deduped
where rn = 1
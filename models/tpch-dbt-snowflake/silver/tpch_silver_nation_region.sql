{{ config(materialized='incremental', incremental_strategy='merge', unique_key='nation_key', schema='tpch_silver') }}

select
    n.nation_key,
    n.nation_name,
    r.region_key,
    r.region_name,
    n._loaded_at
from {{ ref('stg_tpch__nation') }} n
join {{ ref('stg_tpch__region') }} r on n.region_key = r.region_key
qualify row_number() over (partition by n.nation_key order by n._loaded_at desc) = 1
{{ config(materialized='incremental', incremental_strategy='merge', unique_key='part_key', schema='tpch_gold') }}

select
    p.part_key,
    p.part_name,
    p.manufacturer,
    p.brand,
    p.part_type,
    p.part_size,
    p.container,
    p.retail_price,
    avg(ps.supply_cost)   as avg_supply_cost,
    sum(ps.available_qty) as total_available_qty
from {{ ref('tpch_silver_part') }} p
left join {{ ref('tpch_silver_partsupp') }} ps on p.part_key = ps.part_key
group by 1,2,3,4,5,6,7,8
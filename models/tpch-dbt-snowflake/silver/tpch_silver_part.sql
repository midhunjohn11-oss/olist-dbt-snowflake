{{ config(materialized='incremental', incremental_strategy='merge', unique_key='part_key', schema='tpch_silver') }}

with source as (
    select * from {{ ref('stg_tpch__part') }}
    {% if is_incremental() %}
    where _loaded_at > (select coalesce(max(_loaded_at), '1900-01-01'::timestamp) from {{ this }})
    {% endif %}
),

deduped as (
    select
        part_key, part_name, manufacturer, brand, part_type, part_size, container, retail_price,
        _loaded_at,
        row_number() over (partition by part_key order by _loaded_at desc) as rn
    from source
    where part_key is not null
      and retail_price >= 0
)

select part_key, part_name, manufacturer, brand, part_type, part_size, container, retail_price, _loaded_at
from deduped
where rn = 1
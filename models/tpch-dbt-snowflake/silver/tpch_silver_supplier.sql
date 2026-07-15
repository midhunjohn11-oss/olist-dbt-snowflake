{{ config(materialized='incremental', incremental_strategy='merge', unique_key='supplier_key', schema='tpch_silver') }}

with source as (
    select * from {{ ref('stg_tpch__supplier') }}
    {% if is_incremental() %}
    where _loaded_at > (select coalesce(max(_loaded_at), '1900-01-01'::timestamp) from {{ this }})
    {% endif %}
),

deduped as (
    select
        supplier_key,
        trim(supplier_name)    as supplier_name,
        trim(supplier_address) as supplier_address,
        nation_key,
        supplier_phone,
        account_balance,
        _loaded_at,
        row_number() over (partition by supplier_key order by _loaded_at desc) as rn
    from source
    where supplier_key is not null
      and trim(supplier_name) != ''
)

select supplier_key, supplier_name, supplier_address, nation_key, supplier_phone,
       account_balance, _loaded_at
from deduped
where rn = 1
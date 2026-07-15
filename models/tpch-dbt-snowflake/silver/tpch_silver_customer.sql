{{ config(materialized='incremental', incremental_strategy='merge', unique_key='customer_key', schema='tpch_silver') }}

with source as (
    select * from {{ ref('stg_tpch__customer') }}
    {% if is_incremental() %}
    where _loaded_at > (select coalesce(max(_loaded_at), '1900-01-01'::timestamp) from {{ this }})
    {% endif %}
),

deduped as (
    select
        customer_key,
        trim(customer_name)         as customer_name,
        trim(customer_address)      as customer_address,
        nation_key,
        customer_phone,
        account_balance,
        upper(trim(market_segment)) as market_segment,
        _loaded_at,
        row_number() over (partition by customer_key order by _loaded_at desc) as rn
    from source
    where customer_key is not null
      and trim(customer_name) != ''
      and account_balance is not null
)

select customer_key, customer_name, customer_address, nation_key, customer_phone,
       account_balance, market_segment, _loaded_at
from deduped
where rn = 1
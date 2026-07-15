{{ config(materialized='incremental', incremental_strategy='merge', unique_key='order_key', schema='tpch_silver') }}

with source as (
    select * from {{ ref('stg_tpch__orders') }}
    {% if is_incremental() %}
    where _loaded_at > (select coalesce(max(_loaded_at), '1900-01-01'::timestamp) from {{ this }})
    {% endif %}
),

deduped as (
    select
        order_key,
        customer_key,
        case order_status
            when 'O' then 'Open'
            when 'F' then 'Fulfilled'
            when 'P' then 'Partially Filled'
            else 'Unknown'
        end                 as order_status,
        total_price,
        order_date,
        order_priority,
        clerk,
        ship_priority,
        year(order_date)    as order_year,
        month(order_date)   as order_month,
        _loaded_at,
        row_number() over (partition by order_key order by _loaded_at desc) as rn
    from source
    where order_key is not null
      and customer_key is not null
      and total_price >= 0
      and order_date is not null
      and order_date <= current_date()
)

select order_key, customer_key, order_status, total_price, order_date, order_priority,
       clerk, ship_priority, order_year, order_month, _loaded_at
from deduped
where rn = 1
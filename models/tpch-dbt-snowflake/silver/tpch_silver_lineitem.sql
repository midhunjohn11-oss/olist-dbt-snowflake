{{ config(materialized='incremental', incremental_strategy='merge', unique_key=['order_key','line_number'], cluster_by=['ship_date'], schema='tpch_silver') }}

with source as (
    select * from {{ ref('stg_tpch__lineitem') }}
    {% if is_incremental() %}
    where _loaded_at > (select coalesce(max(_loaded_at), '1900-01-01'::timestamp) from {{ this }})
    {% endif %}
),

deduped as (
    select
        order_key, part_key, supplier_key, line_number,
        quantity, extended_price, discount, tax,
        return_flag, line_status,
        ship_date, commit_date, receipt_date,
        ship_instructions, ship_mode,
        round(extended_price * (1 - discount), 2)            as net_revenue,
        round(extended_price * (1 - discount) * (1 + tax), 2) as gross_revenue,
        datediff('day', commit_date, receipt_date)            as days_late,
        _loaded_at,
        row_number() over (partition by order_key, line_number order by _loaded_at desc) as rn
    from source
    where order_key is not null
      and line_number is not null
      and quantity > 0
      and extended_price >= 0
      and discount between 0 and 1
      and tax between 0 and 1
      and return_flag in ('A','N','R')
      and line_status in ('O','F')
      and ship_date is not null
      and receipt_date >= ship_date
)

select order_key, part_key, supplier_key, line_number, quantity, extended_price, discount, tax,
       return_flag, line_status, ship_date, commit_date, receipt_date, ship_instructions, ship_mode,
       net_revenue, gross_revenue, days_late, _loaded_at
from deduped
where rn = 1
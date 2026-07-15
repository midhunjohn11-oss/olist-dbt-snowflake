{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['order_key','line_number'],
    cluster_by=['ship_date'],
    schema='tpch_gold'
) }}

with source as (
    select * from {{ ref('tpch_silver_lineitem') }}
    {% if is_incremental() %}
    where _loaded_at > (select coalesce(max(_loaded_at), '1900-01-01'::timestamp) from {{ this }})
    {% endif %}
),

joined as (
    select
        li.order_key,
        li.line_number,
        li.part_key,
        li.supplier_key,
        o.customer_key,
        li.ship_date,
        li.commit_date,
        li.receipt_date,
        o.order_date,
        li.quantity,
        li.extended_price,
        li.discount,
        li.tax,
        li.net_revenue,
        li.gross_revenue,
        li.return_flag,
        li.line_status,
        li.ship_mode,
        li.days_late,
        o.order_status,
        o.order_priority,
        li._loaded_at,
        row_number() over (partition by li.order_key, li.line_number order by li._loaded_at desc) as rn
    from source li
    left join {{ ref('tpch_silver_orders') }} o on li.order_key = o.order_key
)

select order_key, line_number, part_key, supplier_key, customer_key, ship_date, commit_date,
       receipt_date, order_date, quantity, extended_price, discount, tax, net_revenue, gross_revenue,
       return_flag, line_status, ship_mode, days_late, order_status, order_priority, _loaded_at
from joined
where rn = 1
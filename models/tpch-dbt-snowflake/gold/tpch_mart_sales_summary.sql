{{ config(materialized='table', schema='tpch_gold') }}

select
    d.year,
    d.quarter,
    c.region_name,
    c.nation_name,
    c.market_segment,
    count(distinct f.order_key)               as order_count,
    sum(f.quantity)                           as total_quantity,
    sum(f.net_revenue)                        as net_revenue,
    sum(f.gross_revenue)                      as gross_revenue,
    sum(f.gross_revenue) - sum(f.net_revenue) as tax_collected
from {{ ref('tpch_fact_lineitem') }} f
join {{ ref('tpch_dim_customer') }} c on f.customer_key = c.customer_key and c.is_current = true
join {{ ref('tpch_dim_date') }} d      on f.order_date = d.date_key
group by 1,2,3,4,5
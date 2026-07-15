{{ config(materialized='table', schema='tpch_gold') }}

select
    c.market_segment,
    c.region_name,
    c.nation_name,
    count(distinct o.order_key) as order_count,
    sum(o.total_net_revenue)    as net_revenue,
    avg(o.total_net_revenue)    as avg_order_value,
    min(o.order_date)           as first_order_date,
    max(o.order_date)           as last_order_date
from {{ ref('tpch_fact_orders') }} o
join {{ ref('tpch_dim_customer') }} c on o.customer_key = c.customer_key and c.is_current = true
group by 1,2,3
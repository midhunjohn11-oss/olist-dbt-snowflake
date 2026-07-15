{{ config(materialized='incremental', incremental_strategy='merge', unique_key='order_key', schema='tpch_gold') }}

select
    order_key,
    customer_key,
    order_date,
    order_status,
    order_priority,
    count(distinct line_number)      as line_item_count,
    sum(quantity)                    as total_quantity,
    sum(net_revenue)                 as total_net_revenue,
    sum(gross_revenue)               as total_gross_revenue,
    avg(discount)                    as avg_discount,
    max(receipt_date)                as last_receipt_date
from {{ ref('tpch_fact_lineitem') }}
{% if is_incremental() %}
where order_date >= (select coalesce(dateadd('day', -3, max(order_date)), '1900-01-01'::date) from {{ this }})
{% endif %}
group by 1,2,3,4,5
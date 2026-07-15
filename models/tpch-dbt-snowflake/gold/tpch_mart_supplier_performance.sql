{{ config(materialized='table', schema='tpch_gold') }}

select
    s.supplier_key,
    s.supplier_name,
    s.nation_name,
    s.region_name,
    count(*)                                                                       as lines_shipped,
    sum(f.net_revenue)                                                             as net_revenue,
    avg(f.days_late)                                                               as avg_days_late,
    sum(case when f.days_late > 0 then 1 else 0 end)                               as late_shipment_count,
    round(sum(case when f.days_late > 0 then 1 else 0 end) * 100.0 / count(*), 2)  as late_shipment_pct
from {{ ref('tpch_fact_lineitem') }} f
join {{ ref('tpch_dim_supplier') }} s on f.supplier_key = s.supplier_key and s.is_current = true
group by 1,2,3,4
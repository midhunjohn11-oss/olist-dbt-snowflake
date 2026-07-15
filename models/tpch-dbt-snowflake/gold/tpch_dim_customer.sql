{{ config(materialized='table', schema='tpch_gold') }}

select
    {{ dbt_utils.generate_surrogate_key(['sc.customer_key', 'sc.dbt_valid_from']) }} as customer_sk,
    sc.customer_key,
    sc.customer_name,
    sc.customer_address,
    sc.customer_phone,
    sc.account_balance,
    sc.market_segment,
    nr.nation_name,
    nr.region_name,
    sc.dbt_valid_from                                          as effective_from,
    sc.dbt_valid_to                                            as effective_to,
    case when sc.dbt_valid_to is null then true else false end as is_current
from {{ ref('snap_tpch_customer') }} sc
left join {{ ref('tpch_silver_nation_region') }} nr on sc.nation_key = nr.nation_key
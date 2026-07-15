{{ config(materialized='table', schema='tpch_gold') }}

select
    {{ dbt_utils.generate_surrogate_key(['ss.supplier_key', 'ss.dbt_valid_from']) }} as supplier_sk,
    ss.supplier_key,
    ss.supplier_name,
    ss.supplier_address,
    ss.supplier_phone,
    ss.account_balance,
    nr.nation_name,
    nr.region_name,
    ss.dbt_valid_from                                          as effective_from,
    ss.dbt_valid_to                                            as effective_to,
    case when ss.dbt_valid_to is null then true else false end as is_current
from {{ ref('snap_tpch_supplier') }} ss
left join {{ ref('tpch_silver_nation_region') }} nr on ss.nation_key = nr.nation_key
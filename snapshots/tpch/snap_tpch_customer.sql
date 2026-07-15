{% snapshot snap_tpch_customer %}
{{
    config(
        target_schema='tpch_silver',
        unique_key='customer_key',
        strategy='check',
        check_cols=['customer_name','customer_address','nation_key','customer_phone','account_balance','market_segment']
    )
}}
select * from {{ ref('tpch_silver_customer') }}
{% endsnapshot %}
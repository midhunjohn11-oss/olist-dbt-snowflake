{% snapshot snap_tpch_supplier %}
{{
    config(
        target_schema='tpch_silver',
        unique_key='supplier_key',
        strategy='check',
        check_cols=['supplier_name','supplier_address','nation_key','supplier_phone','account_balance']
    )
}}
select * from {{ ref('tpch_silver_supplier') }}
{% endsnapshot %}
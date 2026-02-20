{% snapshot accounts_snapshot %}
{{
    config(
      target_schema='analytics',
      unique_key='account_id',
      strategy='check',
      check_cols=['customer_id', 'account_type', 'balance']
    )
}}
select * from {{ ref('stg_accounts') }}
{% endsnapshot %}
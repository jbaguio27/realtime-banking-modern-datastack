{{ config(severity='error', tags=['critical']) }}

-- Critical integrity gate for fresh data only:
-- every recently loaded transaction must map to at least one account record.
-- Allowed exceptions can be managed in BANKING.ANALYTICS.ACCOUNT_ORPHAN_EXCEPTIONS.
with recent_transactions as (
    select
        transaction_id,
        account_id
    from {{ ref('fact_transactions') }}
    where load_timestamp >= dateadd(hour, -24, current_timestamp())
),
accounts as (
    select distinct account_id
    from {{ ref('dim_accounts') }}
),
exceptions as (
    select distinct account_id
    from BANKING.ANALYTICS.ACCOUNT_ORPHAN_EXCEPTIONS
    where is_active = true
)
select
    t.transaction_id,
    t.account_id
from recent_transactions t
left join accounts a on t.account_id = a.account_id
left join exceptions e on t.account_id = e.account_id
where a.account_id is null
  and e.account_id is null

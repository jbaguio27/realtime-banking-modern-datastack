

with transactions as (
    select * from banking.raw.stg_transactions
    
    where transaction_time > (select max(transaction_time) from banking.raw.fact_transactions)
    
),
accounts as (
    select account_id, customer_id
    from banking.analytics.accounts_snapshot
    where dbt_valid_to is null
)

select
    t.transaction_id,
    t.account_id,
    a.customer_id,
    t.amount,
    t.transaction_type,
    t.related_account_id,
    t.status,
    t.transaction_time,
    current_timestamp() as load_timestamp
from transactions t
left join accounts a on t.account_id = a.account_id

    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  

-- Quality test (warning): compare transaction-derived balances with current account
-- balances. This can drift for seeded accounts or historical backfills, so warn only.
with txn_sums as (
    select
        account_id,
        sum(case when transaction_type = 'DEPOSIT' then amount else -amount end) as calculated_balance
    from banking.raw.fact_transactions
    group by 1
),
current_balances as (
    select
        account_id,
        balance
    from banking.raw.dim_accounts
    where is_current = true
)

select
    t.account_id,
    t.calculated_balance,
    b.balance as actual_balance
from txn_sums t
join current_balances b on t.account_id = b.account_id
where abs(t.calculated_balance - b.balance) > 0.01
  
  
      
    ) dbt_internal_test
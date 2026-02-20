

-- Quality check: alert if current account balance goes negative.
select
    account_id,
    balance
from banking.analytics.dim_accounts
where is_current = true
  and balance < 0
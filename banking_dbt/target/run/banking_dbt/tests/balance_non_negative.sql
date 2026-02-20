
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  

-- Quality check: alert if current account balance goes negative.
select
    account_id,
    balance
from banking.analytics.dim_accounts
where is_current = true
  and balance < 0
  
  
      
    ) dbt_internal_test
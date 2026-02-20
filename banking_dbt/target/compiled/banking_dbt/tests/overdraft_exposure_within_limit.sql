

-- Quality test (warning): alert when total overdraft exposure exceeds
-- the configured business threshold.
-- Default threshold: 250000 (override with DBT_BP3_MAX_OVERDRAFT_EXPOSURE)
with exposure as (
    select
        coalesce(sum(case when balance < 0 then abs(balance) else 0 end), 0) as overdraft_exposure
    from banking.raw.dim_accounts
    where is_current = true
)
select *
from exposure
where overdraft_exposure > 250000
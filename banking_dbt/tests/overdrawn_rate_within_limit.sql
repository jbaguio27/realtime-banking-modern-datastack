{{ config(severity='warn', tags=['quality']) }}

-- Quality test (warning): alert when the percentage of overdrawn current accounts
-- exceeds the configured business threshold.
-- Default threshold: 20% (override with DBT_BP3_MAX_OVERDRAWN_RATE_PCT)
with totals as (
    select
        count(*) as total_accounts,
        sum(case when balance < 0 then 1 else 0 end) as overdrawn_accounts
    from {{ ref('dim_accounts') }}
    where is_current = true
),
calc as (
    select
        100.0 * overdrawn_accounts / nullif(total_accounts, 0) as overdrawn_rate_pct
    from totals
)
select *
from calc
where overdrawn_rate_pct > {{ env_var('DBT_BP3_MAX_OVERDRAWN_RATE_PCT', '20') }}


    
    

with all_values as (

    select
        balance as value_field,
        count(*) as n_records

    from banking.analytics.dim_accounts
    group by balance

)

select *
from all_values
where value_field not in (
    0
)



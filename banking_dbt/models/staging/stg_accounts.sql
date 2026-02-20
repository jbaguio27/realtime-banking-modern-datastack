{{ config(materialized='view') }}

with raw_data as (
    select
        v:id::number            as account_id,
        v:customer_id::number   as customer_id,
        v:account_type::string  as account_type,
        v:balance::number(38,2) as balance,
        v:currency::string      as currency,
        v:created_at::timestamp_ntz as created_at
    from {{ source('raw', 'accounts') }}
),
deduplicated as (
    select *,
           row_number() over (
               partition by account_id 
               order by created_at desc
           ) as rn
    from raw_data
)
select * exclude rn from deduplicated where rn = 1
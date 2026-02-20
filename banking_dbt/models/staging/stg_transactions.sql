{{ config(materialized='view') }}

with raw_txns as (
    select
        v:id::number                 as transaction_id,
        v:account_id::number         as account_id,
        v:amount::number(38,2)       as amount,
        v:txn_type::string           as transaction_type,
        -- CHECK THIS LINE:
        v:related_account_id::number as related_account_id, 
        v:status::string             as status,
        v:created_at::timestamp_ntz  as transaction_time
    from {{ source('raw', 'transactions') }}
),
deduped as (
    select *,
           row_number() over (partition by transaction_id order by transaction_time desc) as rn
    from raw_txns
)
select * exclude rn from deduped where rn = 1
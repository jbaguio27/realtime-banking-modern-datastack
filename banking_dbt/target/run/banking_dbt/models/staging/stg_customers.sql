
  create or replace   view banking.raw.stg_customers
  
  
  
  
  as (
    

with raw_data as (
    select
        v:id::number            as customer_id,
        v:first_name::string    as first_name,
        v:last_name::string     as last_name,
        v:email::string         as email,
        v:created_at::timestamp_ntz as created_at
    from BANKING.RAW.customers
),
deduplicated as (
    select *,
           row_number() over (partition by customer_id order by created_at desc) as rn
    from raw_data
)
select * exclude rn from deduplicated where rn = 1
  );


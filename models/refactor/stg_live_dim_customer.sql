-- stg_live_dim_customer.sql
select * from {{ source('live', 'dim_customer') }}

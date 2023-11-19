-- stg_live_dim_payment_terms.sql
select * from {{ source('live', 'dim_payment_terms') }}

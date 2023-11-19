-- staging_dim_payment_terms

{{ config(materialized='view') }}

with source_data as (
    select *
    from {{ source('SAP', 'dim_payment_terms') }}
    where acct_type_s = 'D'
    and ifnull(slt_delete, '') <> 'X'
    and active_flag_s = 'Y'
)

select * from source_data

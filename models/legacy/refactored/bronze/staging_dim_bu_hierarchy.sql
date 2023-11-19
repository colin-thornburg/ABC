-- staging_dim_bu_heirarchy

{{ config(materialized='view') }}

with source_data as (
    select *
    from {{ source('fdh_sap', 'dim_bu_heirarchy') }}
    where active_flag = 'Y'
)

select * from source_data

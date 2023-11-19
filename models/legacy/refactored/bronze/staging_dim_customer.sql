-- staging_dim_customer

{{ config(materialized='view') }}

with source_data as (
    select *
    from {{ source('SAP', 'dim_customer') }}
    -- Additional filtering on source system identifiers can be added here if necessary
)

select * from source_data

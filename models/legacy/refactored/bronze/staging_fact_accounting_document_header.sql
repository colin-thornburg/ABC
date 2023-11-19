-- staging_fact_accounting_document_header

{{ config(materialized='view') }}

with source_data as (
    select *
    from {{ source('SAP', 'FACT_ACCOUNTING_DOCUMENT_HEADER') }}
    where ifnull(slt_delete, '') <> 'X'
    -- Additional filtering on invoice_type can be added here if necessary
)

select * from source_data

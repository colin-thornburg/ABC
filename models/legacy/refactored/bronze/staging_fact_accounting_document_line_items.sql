-- staging_fact_accounting_document_line_items

{{ config(materialized='view') }}

with source_data as (
    select *
    from {{ source('SAP', 'FACT_ACCOUNTING_DOCUMENT_LINE_ITEMS') }}
    -- Basic extraction with possible filtering can be added here
)

select * from source_data
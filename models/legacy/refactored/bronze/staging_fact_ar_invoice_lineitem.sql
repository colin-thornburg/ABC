-- staging_fact_ar_invoice_lineitem

{{ config(materialized='view') }}

with source_data as (
    select *
    from {{ source('SAP', 'FACT_AR_INVOICE_LINEITEM') }}
    where src_system_name = 'sap'
    and ifnull(slt_delete, '') <> 'X'
)

select * from source_data

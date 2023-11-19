-- stg_live_fact_accounting_document_header.sql
select * from {{ source('live', 'FACT_ACCOUNTING_DOCUMENT_HEADER') }}

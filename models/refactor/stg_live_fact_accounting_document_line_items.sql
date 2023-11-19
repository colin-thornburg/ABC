-- stg_live_fact_accounting_document_line_items.sql
select * from {{ source('live', 'FACT_ACCOUNTING_DOCUMENT_LINE_ITEMS') }}

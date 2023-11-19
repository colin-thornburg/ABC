-- stg_live_fact_ar_invoice_lineitem.sql
select * from {{ source('live', 'FACT_AR_INVOICE_LINEITEM') }}

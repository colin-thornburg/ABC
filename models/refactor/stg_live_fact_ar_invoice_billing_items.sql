-- stg_live_fact_ar_invoice_billing_items.sql
select * from {{ source('live', 'FACT_AR_INVOICE_BILLING_ITEMS') }}

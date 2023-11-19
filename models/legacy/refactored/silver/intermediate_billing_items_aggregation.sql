-- intermediate_billing_items_aggregation

{{ config(materialized='view') }}

select
    billing_doc,
    count(billing_document_item) as count_billing_document_item
from {{ ref('staging_fact_ar_invoice_billing_items') }}
group by billing_doc

-- int_level1_invoice_billing_items_agg.sql
SELECT
    BILLING_DOC,
    COUNT(BILLING_DOCUMENT_ITEM) as count_BILLING_DOC
FROM {{ ref('stg_live_fact_ar_invoice_billing_items') }}
WHERE src_system_name = 'sap'
-- Add any additional filters or groupings as needed
GROUP BY BILLING_DOC

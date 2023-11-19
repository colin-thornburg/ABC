-- int_level1_invoice_lineitem_header_join.sql
SELECT
    *
FROM {{ ref('stg_live_fact_ar_invoice_lineitem') }} as lineitem
INNER JOIN {{ ref('stg_live_fact_accounting_document_header') }} as header
ON lineitem.Legal_Entity = header.le_number
-- Add additional join logic and select specific columns as needed

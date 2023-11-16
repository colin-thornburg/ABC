{{ config(materialized='table') }}

SELECT
    document_number,
    SUM(amount) as total_amount,
    COUNT(*) as number_of_line_items
FROM {{ ref('int_invoice_document_header') }}
GROUP BY document_number

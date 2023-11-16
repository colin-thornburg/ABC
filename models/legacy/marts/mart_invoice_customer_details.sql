{{ config(materialized='table') }}

SELECT
    idh.*,
    cli.*
FROM {{ ref('int_invoice_document_header') }} idh
JOIN {{ ref('int_customer_lineitem') }} cli
    ON idh.document_number = cli.document_number

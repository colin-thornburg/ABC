{{ config(materialized='table') }}

SELECT
    lib.*,
    bdh.*
FROM {{ ref('int_lineitem_billing') }} lib
JOIN {{ ref('int_billing_document_header') }} bdh
    ON lib.billing_doc = bdh.document_number

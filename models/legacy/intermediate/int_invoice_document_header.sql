{{ config(materialized='view') }}

SELECT
    fact_ar_invoice_lineitem.*,
    FACT_ACCOUNTING_DOCUMENT_HEADER_sap.*
FROM {{ ref('stg_fact_ar_invoice_lineitem') }} fact_ar_invoice_lineitem
INNER JOIN {{ ref('stg_fact_accounting_document_header') }} FACT_ACCOUNTING_DOCUMENT_HEADER_sap
    ON fact_ar_invoice_lineitem.Legal_Entity = FACT_ACCOUNTING_DOCUMENT_HEADER_sap.le_number
    AND fact_ar_invoice_lineitem.DOCUMENT_NUMBER = FACT_ACCOUNTING_DOCUMENT_HEADER_sap.Document_Number
    AND fact_ar_invoice_lineitem.Fiscal_Year = FACT_ACCOUNTING_DOCUMENT_HEADER_sap.Fiscal_Year
WHERE fact_ar_invoice_lineitem.src_system_name = 'sap'
    AND IFNULL(fact_ar_invoice_lineitem.slt_delete, '') <> 'X'
    AND FACT_ACCOUNTING_DOCUMENT_HEADER_sap.invoice_type NOT IN ('31', '32', '33', '34', '35', '36', 'NA', 'RV', 'SD', 'ZC')

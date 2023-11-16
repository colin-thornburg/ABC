{{ config(materialized='view') }}

SELECT
    fact_AR_INVOICE_BILLING_ITEMS.*,
    FACT_ACCOUNTING_DOCUMENT_HEADER_sap.*
FROM {{ ref('stg_fact_ar_invoice_billing_items') }} fact_AR_INVOICE_BILLING_ITEMS
INNER JOIN 
    (SELECT 
        count(BILLING_DOCUMENT_ITEM) as count_BILLING_DOC, 
        BILLING_DOC 
    FROM {{ ref('stg_fact_ar_invoice_billing_items') }} 
    WHERE src_system_name = 'sap' AND IFNULL(slt_delete, '') <> 'X' 
    GROUP BY BILLING_DOC) B 
ON fact_AR_INVOICE_BILLING_ITEMS.BILLING_DOC = B.BILLING_DOC
INNER JOIN {{ ref('stg_fact_accounting_document_header') }} FACT_ACCOUNTING_DOCUMENT_HEADER_sap
ON fact_AR_INVOICE_BILLING_ITEMS.Legal_Entity = FACT_ACCOUNTING_DOCUMENT_HEADER_sap.le_number
AND fact_AR_INVOICE_BILLING_ITEMS.DOCUMENT_NUMBER = FACT_ACCOUNTING_DOCUMENT_HEADER_sap.Document_Number
AND fact_AR_INVOICE_BILLING_ITEMS.Fiscal_Year = FACT_ACCOUNTING_DOCUMENT_HEADER_sap.Fiscal_Year
WHERE IFNULL(fact_AR_INVOICE_BILLING_ITEMS.slt_delete, '') <> 'X'

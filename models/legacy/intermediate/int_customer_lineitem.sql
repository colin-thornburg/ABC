{{ config(materialized='view') }}

SELECT
    dim_Customer.*,
    fact.*
FROM {{ ref('stg_dim_customer') }} dim_Customer
LEFT JOIN 
    (SELECT 
        *,
        -- Additional columns from the original script
    FROM {{ ref('stg_fact_ar_invoice_lineitem') }} fact_ar_invoice_lineitem
    INNER JOIN {{ ref('stg_fact_accounting_document_header') }} FACT_ACCOUNTING_DOCUMENT_HEADER_sap
        ON fact_ar_invoice_lineitem.Legal_Entity = FACT_ACCOUNTING_DOCUMENT_HEADER_sap.le_number
        AND fact_ar_invoice_lineitem.DOCUMENT_NUMBER = FACT_ACCOUNTING_DOCUMENT_HEADER_sap.Document_Number
        AND fact_ar_invoice_lineitem.Fiscal_Year = FACT_ACCOUNTING_DOCUMENT_HEADER_sap.Fiscal_Year
    WHERE fact_ar_invoice_lineitem.src_system_name = 'sap'
        AND IFNULL(fact_ar_invoice_lineitem.slt_delete, '') <> 'X'
        -- Additional WHERE conditions from the original script
    ) fact
ON dim_Customer.party_number_s = fact.Customer
AND dim_Customer.le_number_s = fact.LEGAL_ENTITY
AND dim_Customer.src_system_name_s = fact.src_system_name

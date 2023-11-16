{{ config(materialized='view') }}

WITH fact_ar_invoice_lineitem_joined AS (
    SELECT 
        fact_ar_invoice_lineitem.*,
        fact_accounting_document_header_sap.*
    FROM {{ source('SAP', 'FACT_AR_INVOICE_LINEITEM') }} AS fact_ar_invoice_lineitem
    INNER JOIN {{ source('SAP', 'FACT_ACCOUNTING_DOCUMENT_HEADER') }} AS fact_accounting_document_header_sap
        ON fact_ar_invoice_lineitem.legal_entity = fact_accounting_document_header_sap.le_number
        AND fact_ar_invoice_lineitem.document_number = fact_accounting_document_header_sap.document_number
        AND fact_ar_invoice_lineitem.fiscal_year = fact_accounting_document_header_sap.fiscal_year
    WHERE 
        fact_ar_invoice_lineitem.src_system_name = 'sap'
        AND IFNULL(fact_ar_invoice_lineitem.slt_delete, '') <> 'X'
        AND fact_accounting_document_header_sap.invoice_type NOT IN ('31', '32', '33', '34', '35', '36', 'NA', 'RV', 'SD', 'ZC')
),

fact_ar_invoice_billing_items_joined AS (
    SELECT 
        fact_ar_invoice_billing_items.*,
        b.count_billing_doc,
        fact_ar_invoice_lineitem3.*
    FROM {{ source('SAP', 'FACT_AR_INVOICE_BILLING_ITEMS') }} AS fact_ar_invoice_billing_items
    INNER JOIN (
        SELECT 
            COUNT(billing_document_item) AS count_billing_doc,
            billing_doc
        FROM {{ source('SAP', 'FACT_AR_INVOICE_BILLING_ITEMS') }}
        WHERE src_system_name = 'sap' AND IFNULL(slt_delete, '') <> 'X'
        GROUP BY billing_doc
    ) AS b ON fact_ar_invoice_billing_items.billing_doc = b.billing_doc
    INNER JOIN (
        SELECT 
            fact_ar_invoice_lineitem2.*
        FROM {{ ref('fact_ar_invoice_lineitem_joined') }} AS fact_ar_invoice_lineitem2
        INNER JOIN {{ source('SAP', 'FACT_AR_INVOICE_LINEITEM') }} AS fact_ar_invoice_lineitem1
            ON fact_ar_invoice_lineitem2.billing_doc = fact_ar_invoice_lineitem1.billing_doc
            AND fact_ar_invoice_lineitem2.document_number = fact_ar_invoice_lineitem1.document_number
            AND fact_ar_invoice_lineitem2.rank1 = 1
        WHERE 
            IFNULL(fact_ar_invoice_lineitem2.slt_delete, '') <> 'X'
            AND fact_ar_invoice_lineitem2.src_system_name = 'sap'
    ) AS fact_ar_invoice_lineitem3 ON fact_ar_invoice_billing_items.billing_doc = fact_ar_invoice_lineitem3.billing_doc
        AND fact_ar_invoice_billing_items.billing_doc = fact_ar_invoice_lineitem3.document_number
        AND fact_ar_invoice_lineitem3.src_system_name = fact_ar_invoice_billing_items.src_system_name
)

SELECT 
    *
FROM fact_ar_invoice_lineitem_joined

UNION ALL

SELECT 
    *
FROM fact_ar_invoice_billing_items_joined

With FactAccountingDocumentHeaderSap AS (
    SELECT *
    FROM {{ source('LIVE', 'FACT_ACCOUNTING_DOCUMENT_HEADER') }}
    WHERE invoice_type NOT IN ('31', '32', '33', '34', '35', '36', 'NA', 'RV', 'SD', 'ZC')
)

Select * from FactAccountingDocumentHeaderSap
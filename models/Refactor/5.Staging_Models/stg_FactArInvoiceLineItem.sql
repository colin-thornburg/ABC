WITH FactArInvoiceLineItem AS (
    SELECT *
    FROM {{ source('LIVE', 'FACT_AR_INVOICE_LINEITEM') }}
    WHERE src_system_name = 'sap'
      AND ifnull(slt_delete, '') <> 'X'
)

Select * from FacArInvoiceLineItem
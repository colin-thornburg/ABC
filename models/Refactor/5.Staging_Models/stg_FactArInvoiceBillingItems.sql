With FactArInvoiceBillingItems AS (
    SELECT *
    FROM {{ source('LIVE', 'FACT_AR_INVOICE_BILLING_ITEMS') }}
    WHERE src_system_name = 'sap'
      AND ifnull(slt_delete, '') <> 'X'
)

Select * from FactArInvoiceBillingItems
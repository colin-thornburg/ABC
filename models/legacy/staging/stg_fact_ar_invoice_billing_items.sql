{{ config(materialized='view') }}

SELECT *
FROM {{ source('SAP', 'FACT_AR_INVOICE_BILLING_ITEMS') }}

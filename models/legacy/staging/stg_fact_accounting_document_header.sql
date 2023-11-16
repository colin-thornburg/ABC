{{ config(materialized='view') }}

SELECT *
FROM {{ source('SAP', 'FACT_ACCOUNTING_DOCUMENT_HEADER') }}

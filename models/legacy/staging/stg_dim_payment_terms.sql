{{ config(materialized='view') }}

SELECT *
FROM {{ source('SAP', 'dim_payment_terms') }}

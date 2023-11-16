{{ config(materialized='view') }}

SELECT * FROM {{ ref('stg_fact_ar_invoice_lineitem') }}
UNION ALL
SELECT * FROM {{ ref('stg_fact_ar_invoice_billing_items') }}

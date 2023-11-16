{{ config(materialized='table') }}

SELECT
    customer,
    SUM(amount) as total_customer_amount,
    COUNT(*) as number_of_transactions
FROM {{ ref('int_customer_lineitem') }}
GROUP BY customer

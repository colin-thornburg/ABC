{{ config(materialized='incremental', unique_key='policy_key') }}

WITH revenue_fact_base AS (
    SELECT
        policy_key,
        bu_key,
        client_key,
        carrier_insurer_key,
        carrier_payee_key,
        product_key,
        product_line_key,
        invoice_date_key,
        bill_type_key,
        SUM(agent_commission_amt_usd) AS agent_commission_amt_usd,
        SUM(billed_premium_amt_usd) AS billed_premium_amt_usd,
        SUM(brokerage_expense_amt_usd) AS brokerage_expense_amt_usd,
        SUM(commission_revenue_amt_usd) AS commission_revenue_amt_usd,
        SUM(fee_revenue_amt_usd) AS fee_revenue_amt_usd
    FROM {{ ref('stg_edw__f_revenue_detail') }} a
    INNER JOIN {{ ref('stg_edw__d_carrier') }} b
        ON a.carrier_payee_key = b.carrier_key
    WHERE a.env_source_code = 'EPIC_US'
        
    --WHERE a.env_source_code = ' var("env_source_code_EPIC_US") }}'
     --   OR a.env_source_code = ' var("env_source_code_FDW") }}'
    {% if is_incremental() %}
        AND a.invoice_date_key > (SELECT MAX(invoice_date_key) FROM {{ this }})
    {% endif %}
    GROUP BY 1,2,3,4,5,6,7,8,9
)
SELECT 
    policy_key,
    bu_key,
    client_key,
    carrier_insurer_key,
    carrier_payee_key,
    product_key,
    product_line_key,
    invoice_date_key,
    bill_type_key,
    agent_commission_amt_usd,
    billed_premium_amt_usd,
    brokerage_expense_amt_usd,
    commission_revenue_amt_usd,
    fee_revenue_amt_usd,
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM revenue_fact_base
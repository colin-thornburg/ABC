{{ config(materialized='view') }}

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
    env_source_code,
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM {{ ref('f_revenue_detail') }}
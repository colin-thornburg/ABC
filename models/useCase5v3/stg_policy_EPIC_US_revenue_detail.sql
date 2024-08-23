-- models/staging/stg_policy_EPIC_US_revenue_detail.sql

{{ config(materialized='view') }}

WITH source_data AS (
    SELECT
        policy_key,
        bu_key,
        bu_department_key,
        bu_state_key,
        client_key,
        carrier_insurer_key,
        carrier_payee_key,
        product_key,
        product_line_key,
        producer_key,
        client_producer_key,
        client_account_manager_key,
        invoice_date_key,
        bill_type_key,
        agent_commission_amt_lcl,
        agent_commission_amt_usd,
        agent_commission_amt_pegusd,
        agent_commission_amt_trns,
        billed_premium_amt_lcl,
        billed_premium_amt_usd,
        billed_premium_amt_pegusd,
        billed_premium_amt_trns,
        brokerage_expense_amt_lcl,
        brokerage_expense_amt_usd,
        brokerage_expense_amt_pegusd,
        brokerage_expense_amt_trns,
        commission_revenue_amt_lcl,
        commission_revenue_amt_usd,
        commission_revenue_amt_pegusd,
        commission_revenue_amt_trns,
        fee_revenue_amt_lcl,
        fee_revenue_amt_usd,
        fee_revenue_amt_pegusd,
        fee_revenue_amt_trns,
        env_source_code
 --   FROM { source('edw', 'f_revenue_detail') }}
    FROM {{ ref('edw_f_revenue_detail') }}
    WHERE env_source_code = 'EPIC_US' -- Hardcoded for now, could be made dynamic
       OR env_source_code = 'FDW' -- Additional source code, also could be made dynamic
)

SELECT * FROM source_data
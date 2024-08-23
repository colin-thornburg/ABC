-- models/intermediate/int_policy_EPIC_US_revenue_detail.sql

{{ config(materialized='table') }}

WITH revenue_fact_base AS (
    SELECT
        r.*,
        CASE
            WHEN r.commission_revenue_amt_lcl <> 0 AND r.commission_revenue_amt_lcl <> r.billed_premium_amt_lcl THEN r.billed_premium_amt_lcl
            ELSE 0
        END AS commission_premium_amt_lcl,
        CASE
            WHEN r.commission_revenue_amt_usd <> 0 AND r.commission_revenue_amt_usd <> r.billed_premium_amt_usd THEN r.billed_premium_amt_usd
            ELSE 0
        END AS commission_premium_amt_usd,
        CASE
            WHEN r.commission_revenue_amt_pegusd <> 0 AND r.commission_revenue_amt_pegusd <> r.billed_premium_amt_pegusd THEN r.billed_premium_amt_pegusd
            ELSE 0
        END AS commission_premium_amt_pegusd,
        CASE
            WHEN r.commission_revenue_amt_trns <> 0 AND r.commission_revenue_amt_trns <> r.billed_premium_amt_trns THEN r.billed_premium_amt_trns
            ELSE 0
        END AS commission_premium_amt_trns,
        CASE
            WHEN r.commission_revenue_amt_lcl = 0 OR r.commission_revenue_amt_lcl = r.billed_premium_amt_lcl THEN r.billed_premium_amt_lcl
            ELSE 0
        END AS fee_premium_amt_lcl,
        CASE
            WHEN r.commission_revenue_amt_usd = 0 OR r.commission_revenue_amt_usd = r.billed_premium_amt_usd THEN r.billed_premium_amt_usd
            ELSE 0
        END AS fee_premium_amt_usd,
        CASE
            WHEN r.commission_revenue_amt_pegusd = 0 OR r.commission_revenue_amt_pegusd = r.billed_premium_amt_pegusd THEN r.billed_premium_amt_pegusd
            ELSE 0
        END AS fee_premium_amt_pegusd,
        CASE
            WHEN r.commission_revenue_amt_trns = 0 OR r.commission_revenue_amt_trns = r.billed_premium_amt_trns THEN r.billed_premium_amt_trns
            ELSE 0
        END AS fee_premium_amt_trns,
        r.commission_revenue_amt_usd + r.fee_revenue_amt_usd + r.brokerage_expense_amt_usd + r.agent_commission_amt_usd AS revenue_amt_lcl,
        CASE
            WHEN IFNULL(c.carrier_master_parent_name, '') ILIKE 'Gallagher Global Brokerage-US' THEN 0
            WHEN UPPER(b.region_name) ILIKE 'GGB ANZ - Broking NZ' AND (cl.client_id ILIKE 'MNZ%' OR c.carrier_name ILIKE 'Certain Underwriters at Lloyd''s (B1262BW0127720)') THEN 0
            ELSE 1
        END AS premium_amt_factor
    FROM {{ ref('stg_policy_EPIC_US_revenue_detail') }} r
    LEFT JOIN {{ ref('d_carrier') }} c ON r.carrier_payee_key = c.carrier_key
    LEFT JOIN {{ ref('d_client') }} cl ON r.client_key = cl.client_key
    LEFT JOIN {{ ref('d_bu') }} b ON r.bu_key = b.bu_key
),

revenue_fact_main AS (
    SELECT
        *,
        SUM(revenue_amt_lcl) OVER (PARTITION BY policy_key) AS total_revenue_amt_lcl,
        SUM(revenue_amt_lcl) OVER (PARTITION BY policy_key, bu_key) AS revenue_amt_bu,
        SUM(revenue_amt_lcl) OVER (PARTITION BY policy_key, client_key) AS revenue_amt_client,
        SUM(revenue_amt_lcl) OVER (PARTITION BY policy_key, carrier_insurer_key) AS revenue_amt_carrier_insurer,
        SUM(revenue_amt_lcl) OVER (PARTITION BY policy_key, carrier_payee_key) AS revenue_amt_carrier_payee,
        SUM(revenue_amt_lcl) OVER (PARTITION BY policy_key, product_key) AS revenue_amt_product,
        SUM(revenue_amt_lcl) OVER (PARTITION BY policy_key, product_line_key) AS revenue_amt_product_line,
        SUM(revenue_amt_lcl) OVER (PARTITION BY policy_key, producer_key) AS revenue_amt_producer01,
        SUM(revenue_amt_lcl) OVER (PARTITION BY policy_key, client_producer_key) AS revenue_amt_client_producer,
        SUM(revenue_amt_lcl) OVER (PARTITION BY policy_key, client_account_manager_key) AS revenue_amt_client_account_manager,
        SUM(revenue_amt_lcl) OVER (PARTITION BY policy_key, invoice_date_key) AS revenue_amt_invoice_date,
        SUM(revenue_amt_lcl) OVER (PARTITION BY policy_key, bill_type_key) AS revenue_amt_billing_type
    FROM revenue_fact_base
)

SELECT DISTINCT
    policy_key,
    FIRST_VALUE(bu_key) OVER (PARTITION BY policy_key ORDER BY revenue_amt_bu DESC, bu_key) AS bu_key,
    FIRST_VALUE(bu_department_key) OVER (PARTITION BY policy_key ORDER BY revenue_amt_bu DESC, bu_key) AS bu_department_key,
    FIRST_VALUE(bu_state_key) OVER (PARTITION BY policy_key ORDER BY revenue_amt_bu DESC, bu_key) AS bu_state_key,
    FIRST_VALUE(client_key) OVER (PARTITION BY policy_key ORDER BY revenue_amt_client DESC, client_key) AS client_key,
    FIRST_VALUE(carrier_insurer_key) OVER (PARTITION BY policy_key ORDER BY revenue_amt_carrier_insurer DESC, carrier_insurer_key) AS carrier_insurer_key,
    FIRST_VALUE(carrier_payee_key) OVER (PARTITION BY policy_key ORDER BY revenue_amt_carrier_payee DESC, carrier_payee_key) AS carrier_payee_key,
    FIRST_VALUE(product_key) OVER (PARTITION BY policy_key ORDER BY revenue_amt_product DESC, product_key) AS product_key,
    FIRST_VALUE(product_line_key) OVER (PARTITION BY policy_key ORDER BY revenue_amt_product_line DESC, product_line_key) AS product_line_key,
    FIRST_VALUE(producer_key) OVER (PARTITION BY policy_key ORDER BY revenue_amt_producer01 DESC, producer_key) AS producer_01_key,
    FIRST_VALUE(client_producer_key) OVER (PARTITION BY policy_key ORDER BY revenue_amt_client_producer DESC, client_producer_key) AS client_producer_key,
    FIRST_VALUE(client_account_manager_key) OVER (PARTITION BY policy_key ORDER BY revenue_amt_client_account_manager DESC, client_account_manager_key) AS client_account_manager_key,
    FIRST_VALUE(invoice_date_key) OVER (PARTITION BY policy_key ORDER BY revenue_amt_invoice_date DESC, invoice_date_key) AS invoice_date_key,
    FIRST_VALUE(bill_type_key) OVER (
        PARTITION BY policy_key 
        ORDER BY 
            revenue_amt_billing_type DESC, 
            CASE WHEN bill_type_key IN (SELECT bill_type_key FROM {{ ref('d_bill_type') }} WHERE bill_type_desc = 'Direct Bill') THEN 1 ELSE 2 END,
            bill_type_key
    ) AS bill_type_key,
    SUM(agent_commission_amt_lcl) AS agent_commission_amt_lcl,
    SUM(agent_commission_amt_usd) AS agent_commission_amt_usd,
    SUM(agent_commission_amt_pegusd) AS agent_commission_amt_pegusd,
    SUM(agent_commission_amt_trns) AS agent_commission_amt_trns,
    SUM(billed_premium_amt_lcl * premium_amt_factor) AS billed_premium_amt_lcl,
    SUM(billed_premium_amt_usd * premium_amt_factor) AS billed_premium_amt_usd,
    SUM(billed_premium_amt_pegusd * premium_amt_factor) AS billed_premium_amt_pegusd,
    SUM(billed_premium_amt_trns * premium_amt_factor) AS billed_premium_amt_trns,
    SUM(brokerage_expense_amt_lcl) AS brokerage_expense_amt_lcl,
    SUM(brokerage_expense_amt_usd) AS brokerage_expense_amt_usd,
    SUM(brokerage_expense_amt_pegusd) AS brokerage_expense_amt_pegusd,
    SUM(brokerage_expense_amt_trns) AS brokerage_expense_amt_trns,
    SUM(commission_revenue_amt_lcl) AS commission_revenue_amt_lcl,
    SUM(commission_revenue_amt_usd) AS commission_revenue_amt_usd,
    SUM(commission_revenue_amt_pegusd) AS commission_revenue_amt_pegusd,
    SUM(commission_revenue_amt_trns) AS commission_revenue_amt_trns,
    SUM(fee_revenue_amt_lcl) AS fee_revenue_amt_lcl,
    SUM(fee_revenue_amt_usd) AS fee_revenue_amt_usd,
    SUM(fee_revenue_amt_pegusd) AS fee_revenue_amt_pegusd,
    SUM(fee_revenue_amt_trns) AS fee_revenue_amt_trns,
    SUM(commission_premium_amt_lcl * premium_amt_factor) AS commission_premium_amt_lcl,
    SUM(commission_premium_amt_usd * premium_amt_factor) AS commission_premium_amt_usd,
    SUM(commission_premium_amt_pegusd * premium_amt_factor) AS commission_premium_amt_pegusd,
    SUM(commission_premium_amt_trns * premium_amt_factor) AS commission_premium_amt_trns,
    SUM(fee_premium_amt_lcl * premium_amt_factor) AS fee_premium_amt_lcl,
    SUM(fee_premium_amt_usd * premium_amt_factor) AS fee_premium_amt_usd,
    SUM(fee_premium_amt_pegusd * premium_amt_factor) AS fee_premium_amt_pegusd,
    SUM(fee_premium_amt_trns * premium_amt_factor) AS fee_premium_amt_trns
FROM revenue_fact_main
GROUP BY policy_key
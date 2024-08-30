-- This model replaces the final SELECT statement in the stored procedure
SELECT DISTINCT
    a.policy_key,
    a.agent_commission_amt_lcl,
    a.agent_commission_amt_usd,
    a.agent_commission_amt_pegusd,
    a.agent_commission_amt_trns,
    a.billed_premium_amt_lcl,
    a.billed_premium_amt_usd,
    a.billed_premium_amt_pegusd,
    a.billed_premium_amt_trns,
    a.brokerage_expense_amt_lcl,
    a.brokerage_expense_amt_usd,
    a.brokerage_expense_amt_pegusd,
    a.brokerage_expense_amt_trns,
    a.commission_revenue_amt_lcl,
    a.commission_revenue_amt_usd,
    a.commission_revenue_amt_pegusd,
    a.commission_revenue_amt_trns,
    a.fee_revenue_amt_lcl,
    a.fee_revenue_amt_usd,
    a.fee_revenue_amt_pegusd,
    a.fee_revenue_amt_trns,
    a.commission_premium_amt_lcl,
    a.commission_premium_amt_usd,
    a.commission_premium_amt_pegusd,
    a.commission_premium_amt_trns,
    a.fee_premium_amt_lcl,
    a.fee_premium_amt_usd,
    a.fee_premium_amt_pegusd,
    a.fee_premium_amt_trns,
    CASE
        WHEN
            SUM(COALESCE(billed_premium_amt_lcl, 0))
                OVER (PARTITION BY policy_key, bu_key)
            = 0
            AND SUM(COALESCE(revenue_amt_lcl, 0))
                OVER (PARTITION BY policy_key, bu_key)
            = 0
            THEN 2
        ELSE 1
    END AS priority_order_bu,
    FIRST_VALUE(a.bu_key) OVER (
        PARTITION BY a.policy_key
        ORDER BY
            a.priority_order_bu ASC,
            a.revenue_amt_bu DESC NULLS LAST,
            a.bu_key ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS bu_key,
    FIRST_VALUE(a.bu_department_key) OVER (
        PARTITION BY a.policy_key
        ORDER BY
            a.priority_order_bu ASC,
            a.revenue_amt_bu DESC NULLS LAST,
            a.bu_key ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS bu_department_key,
    FIRST_VALUE(a.bu_state_key) OVER (
        PARTITION BY a.policy_key
        ORDER BY
            a.priority_order_bu ASC,
            a.revenue_amt_bu DESC NULLS LAST,
            a.bu_key ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS bu_state_key,
    FIRST_VALUE(a.client_key) OVER (
        PARTITION BY a.policy_key
        ORDER BY
            a.priority_order_client ASC,
            a.revenue_amt_client DESC NULLS LAST,
            a.client_key ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS client_key,
    FIRST_VALUE(a.carrier_insurer_key) OVER (
        PARTITION BY a.policy_key
        ORDER BY
            a.priority_order_carrier_insurer ASC,
            a.revenue_amt_carrier_insurer DESC NULLS LAST,
            a.carrier_insurer_key ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS carrier_insurer_key,
    FIRST_VALUE(a.carrier_payee_key) OVER (
        PARTITION BY a.policy_key
        ORDER BY
            a.priority_order_carrier_payee ASC,
            a.revenue_amt_carrier_payee DESC NULLS LAST,
            a.carrier_payee_key ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS carrier_payee_key,
    FIRST_VALUE(a.product_key) OVER (
        PARTITION BY a.policy_key
        ORDER BY
            a.priority_order_product ASC,
            a.revenue_amt_product DESC NULLS LAST,
            a.product_key ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS product_key,
    FIRST_VALUE(a.product_line_key) OVER (
        PARTITION BY a.policy_key
        ORDER BY
            a.priority_order_product_line ASC,
            a.revenue_amt_product_line DESC NULLS LAST,
            a.product_line_key ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS product_line_key,
    FIRST_VALUE(a.producer01_key) OVER (
        PARTITION BY a.policy_key
        ORDER BY
            a.priority_order_producer01 ASC,
            a.revenue_amt_producer01 DESC NULLS LAST,
            a.producer01_key ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS producer_01_key,
    FIRST_VALUE(a.client_producer_key) OVER (
        PARTITION BY a.policy_key
        ORDER BY
            a.priority_order_client_producer ASC,
            a.revenue_amt_client_producer DESC NULLS LAST,
            a.client_producer_key ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS client_producer_key,
    FIRST_VALUE(a.client_account_manager_key) OVER (
        PARTITION BY a.policy_key
        ORDER BY
            a.priority_order_client_account_manager ASC,
            a.revenue_amt_client_account_manager DESC NULLS LAST,
            a.client_account_manager_key ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS client_account_manager_key,
    FIRST_VALUE(a.invoice_date_key) OVER (
        PARTITION BY a.policy_key
        ORDER BY
            a.priority_order_invoice_date ASC,
            a.revenue_amt_invoice_date DESC NULLS LAST,
            a.invoice_date_key ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS invoice_date_key,
    FIRST_VALUE(a.bill_type_key) OVER (
        PARTITION BY a.policy_key
        ORDER BY
            a.priority_order_billing_type ASC,
            a.revenue_amt_billing_type DESC NULLS LAST,
            CASE WHEN b.bill_type_desc = 'Direct Bill' THEN 1 ELSE 2 END,
            a.bill_type_key ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS bill_type_key
FROM {{ ref('revenue_fact_main') }} AS a
INNER JOIN {{ ref('fdw_s_dim_bill_type') }} AS b
    ON a.bill_type_key = b.bill_type_key

SELECT
    a.policy_key,
    CASE 
        WHEN TO_VARCHAR(a.client_key) <= 'unknown_key' THEN IFNULL(TO_VARCHAR(b.client_key), 'unknown_key') 
        ELSE TO_VARCHAR(a.client_key) 
    END AS client_key,
    CASE 
        WHEN TO_VARCHAR(a.carrier_insurer_key) <= 'unknown_key' THEN IFNULL(TO_VARCHAR(b.carrier_insurer_key), 'unknown_key') 
        ELSE TO_VARCHAR(a.carrier_insurer_key) 
    END AS carrier_insurer_key,
    CASE 
        WHEN TO_VARCHAR(a.carrier_payee_key) <= 'unknown_key' THEN IFNULL(TO_VARCHAR(b.carrier_payee_key), 'unknown_key') 
        ELSE TO_VARCHAR(a.carrier_payee_key) 
    END AS carrier_payee_key,
    CASE 
        WHEN TO_VARCHAR(a.product_key) <= 'unknown_key' THEN IFNULL(TO_VARCHAR(b.product_key), 'unknown_key') 
        ELSE TO_VARCHAR(a.product_key) 
    END AS product_key,
    CASE 
        WHEN TO_VARCHAR(a.product_line_key) <= 'unknown_key' THEN IFNULL(TO_VARCHAR(b.product_line_key), 'unknown_key') 
        ELSE TO_VARCHAR(a.product_line_key) 
    END AS product_line_key,
    CASE 
        WHEN TO_VARCHAR(a.producer_01_key) <= 'unknown_key' THEN IFNULL(TO_VARCHAR(b.producer_01_key), 'unknown_key') 
        ELSE TO_VARCHAR(a.producer_01_key) 
    END AS producer_01_key,
    TO_VARCHAR(a.producer_02_key) AS producer_02_key,
    TO_VARCHAR(a.account_manager_key) AS account_manager_key,
    TO_VARCHAR(a.effective_date_key) AS effective_date_key,
    TO_VARCHAR(a.expiration_date_key) AS expiration_date_key,
    TO_VARCHAR(a.inception_date_key) AS inception_date_key,
    'unknown_key' AS cancel_date_key,
    'unknown_key' AS reinstated_date_key,
    IFNULL(TO_VARCHAR(b.invoice_date_key), 'unknown_key') AS invoice_date_key,
    TO_VARCHAR(a.contracted_expiration_date_key) AS contracted_expiration_date_key,
    CASE 
        WHEN TO_VARCHAR(a.client_producer_key) <= 'unknown_key' THEN IFNULL(TO_VARCHAR(b.client_producer_key), 'unknown_key') 
        ELSE TO_VARCHAR(a.client_producer_key) 
    END AS client_producer_key,
    CASE 
        WHEN TO_VARCHAR(a.client_account_manager_key) <= 'unknown_key' THEN IFNULL(TO_VARCHAR(b.client_account_manager_key), 'unknown_key') 
        ELSE TO_VARCHAR(a.client_account_manager_key) 
    END AS client_account_manager_key,
    CASE 
        WHEN IFNULL(TO_VARCHAR(b.bu_key), 'unknown_key') <= 'unknown_key' THEN TO_VARCHAR(a.bu_key) 
        ELSE IFNULL(TO_VARCHAR(b.bu_key), 'unknown_key') 
    END AS bu_key,
    CASE 
        WHEN IFNULL(TO_VARCHAR(b.bu_department_key), 'unknown_key') <= 'unknown_key' THEN TO_VARCHAR(a.bu_department_key) 
        ELSE IFNULL(TO_VARCHAR(b.bu_department_key), 'unknown_key') 
    END AS bu_department_key,
    CASE 
        WHEN IFNULL(TO_VARCHAR(b.bu_state_key), 'unknown_key') <= 'unknown_key' THEN TO_VARCHAR(a.bu_state_key) 
        ELSE IFNULL(TO_VARCHAR(b.bu_state_key), 'unknown_key') 
    END AS bu_state_key,
    'unknown_key' AS renewal_date_key,
    'unknown_key' AS financing_company_key,
    'unknown_key' AS excess_policy_key,
    TO_VARCHAR(a.source_system_key) AS source_system_key,
    TO_VARCHAR(a.source_system_instance_key) AS source_system_instance_key,
    CASE 
        WHEN TO_VARCHAR(a.bill_type_key) <= 'unknown_key' THEN IFNULL(TO_VARCHAR(b.bill_type_key), 'unknown_key') 
        ELSE TO_VARCHAR(a.bill_type_key) 
    END AS bill_type_key,
    a.env_source_code,
    a.data_source_code,
    a.annualized_endorsement_premium_amt,
    a.written_premium_amt,
    a.annualized_premium_amt,
    a.estimated_premium_amt,
    b.agent_commission_amt_lcl,
    b.agent_commission_amt_usd,
    b.agent_commission_amt_pegusd,
    b.agent_commission_amt_trns,
    b.billed_premium_amt_lcl,
    b.billed_premium_amt_usd,
    b.billed_premium_amt_pegusd,
    b.billed_premium_amt_trns,
    b.brokerage_expense_amt_lcl,
    b.brokerage_expense_amt_usd,
    b.brokerage_expense_amt_pegusd,
    b.brokerage_expense_amt_trns,
    b.commission_revenue_amt_lcl,
    b.commission_revenue_amt_usd,
    b.commission_revenue_amt_pegusd,
    b.commission_revenue_amt_trns,
    b.fee_revenue_amt_lcl,
    b.fee_revenue_amt_usd,
    b.fee_revenue_amt_pegusd,
    b.fee_revenue_amt_trns,
    b.commission_premium_amt_lcl,
    b.commission_premium_amt_usd,
    b.commission_premium_amt_pegusd,
    b.commission_premium_amt_trns,
    b.fee_premium_amt_lcl,
    b.fee_premium_amt_usd,
    b.fee_premium_amt_pegusd,
    b.fee_premium_amt_trns,
    a.policy_id,
    a.client_id,
    a.carrier_insurer_id,
    a.carrier_payee_id,
    a.account_manager_code,
    a.producer_01_code,
    a.producer_02_code,
    a.client_account_manager_code,
    a.client_producer_code,
    a.product_id,
    a.product_line_id,
    a.bill_type_code,
    a.department_code,
    a.bu_id,
    a.bu_department_code,
    a.state_code,
    a.effective_date,
    a.expiration_date,
    a.inception_date,
    a.contracted_expiration_date
FROM {{ ref('stg_edw_f_policy_EPIC_US_source') }} AS a
LEFT OUTER JOIN {{ ref('revenue_detail') }} AS b
ON b.policy_key = a.policy_key

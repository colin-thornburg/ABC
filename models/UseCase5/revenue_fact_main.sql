SELECT DISTINCT
			policy_key,
			bu_key,
			bu_department_key,
			bu_state_key,
			client_key,
			carrier_insurer_key,
			carrier_payee_key,
			product_key,
			product_line_key,
			producer_key AS producer01_key,
			client_producer_key,
			client_account_manager_key,
			invoice_date_key,
			bill_type_key,
			SUM(agent_commission_amt_lcl) OVER (PARTITION BY policy_key) AS agent_commission_amt_lcl,
			SUM(agent_commission_amt_usd) OVER (PARTITION BY policy_key) AS agent_commission_amt_usd,
			SUM(agent_commission_amt_pegusd) OVER (PARTITION BY policy_key) AS agent_commission_amt_pegusd,
			SUM(agent_commission_amt_trns) OVER (PARTITION BY policy_key) AS agent_commission_amt_trns,
			SUM(billed_premium_amt_lcl * premium_amt_factor) OVER (PARTITION BY policy_key) AS billed_premium_amt_lcl,
			SUM(billed_premium_amt_usd * premium_amt_factor) OVER (PARTITION BY policy_key) AS billed_premium_amt_usd,
			SUM(billed_premium_amt_pegusd * premium_amt_factor) OVER (PARTITION BY policy_key) AS billed_premium_amt_pegusd,
			SUM(billed_premium_amt_trns * premium_amt_factor) OVER (PARTITION BY policy_key) AS billed_premium_amt_trns,
			SUM(brokerage_expense_amt_lcl) OVER (PARTITION BY policy_key) AS brokerage_expense_amt_lcl,
			SUM(brokerage_expense_amt_usd) OVER (PARTITION BY policy_key) AS brokerage_expense_amt_usd,
			SUM(brokerage_expense_amt_pegusd) OVER (PARTITION BY policy_key) AS brokerage_expense_amt_pegusd,
			SUM(brokerage_expense_amt_trns) OVER (PARTITION BY policy_key) AS brokerage_expense_amt_trns,
			SUM(commission_revenue_amt_lcl) OVER (PARTITION BY policy_key) AS commission_revenue_amt_lcl,
			SUM(commission_revenue_amt_usd) OVER (PARTITION BY policy_key) AS commission_revenue_amt_usd,
			SUM(commission_revenue_amt_pegusd) OVER (PARTITION BY policy_key) AS commission_revenue_amt_pegusd,
			SUM(commission_revenue_amt_trns) OVER (PARTITION BY policy_key) AS commission_revenue_amt_trns,
			SUM(fee_revenue_amt_lcl) OVER (PARTITION BY policy_key) AS fee_revenue_amt_lcl,
			SUM(fee_revenue_amt_usd) OVER (PARTITION BY policy_key) AS fee_revenue_amt_usd,
			SUM(fee_revenue_amt_pegusd) OVER (PARTITION BY policy_key) AS fee_revenue_amt_pegusd,
			SUM(fee_revenue_amt_trns) OVER (PARTITION BY policy_key) AS fee_revenue_amt_trns,
			SUM(commission_premium_amt_lcl * premium_amt_factor) OVER (PARTITION BY policy_key) AS commission_premium_amt_lcl,
			SUM(commission_premium_amt_usd * premium_amt_factor) OVER (PARTITION BY policy_key) AS commission_premium_amt_usd,
			SUM(commission_premium_amt_pegusd * premium_amt_factor) OVER (PARTITION BY policy_key) AS commission_premium_amt_pegusd,
			SUM(commission_premium_amt_trns * premium_amt_factor) OVER (PARTITION BY policy_key) AS commission_premium_amt_trns,
			SUM(fee_premium_amt_lcl * premium_amt_factor) OVER (PARTITION BY policy_key) AS fee_premium_amt_lcl,
			SUM(fee_premium_amt_usd * premium_amt_factor) OVER (PARTITION BY policy_key) AS fee_premium_amt_usd,
			SUM(fee_premium_amt_pegusd * premium_amt_factor) OVER (PARTITION BY policy_key) AS fee_premium_amt_pegusd,
			SUM(fee_premium_amt_trns * premium_amt_factor) OVER (PARTITION BY policy_key) AS fee_premium_amt_trns,
			SUM(revenue_amt_lcl) OVER (PARTITION BY policy_key) AS revenue_amt_lcl,
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
			SUM(revenue_amt_lcl) OVER (PARTITION BY policy_key, bill_type_key) AS revenue_amt_billing_type,
			CASE
				WHEN SUM(IFNULL(billed_premium_amt_lcl, 0)) OVER (PARTITION BY policy_key, bu_key) = 0
				AND SUM(IFNULL(revenue_amt_lcl, 0)) OVER (PARTITION BY policy_key, bu_key) = 0
					THEN 2
				ELSE 1
			END priority_order_bu,
			CASE
				WHEN SUM(IFNULL(billed_premium_amt_lcl, 0)) OVER (PARTITION BY policy_key, client_key) = 0
				AND SUM(IFNULL(revenue_amt_lcl, 0)) OVER (PARTITION BY policy_key, client_key) = 0
					THEN 2
				ELSE 1
			END priority_order_client,
			CASE
				WHEN SUM(IFNULL(billed_premium_amt_lcl, 0)) OVER (PARTITION BY policy_key, carrier_insurer_key) = 0
				AND SUM(IFNULL(revenue_amt_lcl, 0)) OVER (PARTITION BY policy_key, carrier_insurer_key) = 0
					THEN 2
				ELSE 1
			END priority_order_carrier_insurer,
			CASE
				WHEN SUM(IFNULL(billed_premium_amt_lcl, 0)) OVER (PARTITION BY policy_key, carrier_payee_key) = 0
				AND SUM(IFNULL(revenue_amt_lcl, 0)) OVER (PARTITION BY policy_key, carrier_payee_key) = 0
					THEN 2
				ELSE 1
			END priority_order_carrier_payee,
			CASE
				WHEN SUM(IFNULL(billed_premium_amt_lcl, 0)) OVER (PARTITION BY policy_key, product_key) = 0
				AND SUM(IFNULL(revenue_amt_lcl, 0)) OVER (PARTITION BY policy_key, product_key) = 0
					THEN 2
				ELSE 1
			END priority_order_product,
			CASE
				WHEN SUM(IFNULL(billed_premium_amt_lcl, 0)) OVER (PARTITION BY policy_key, product_line_key) = 0
				AND SUM(IFNULL(revenue_amt_lcl, 0)) OVER (PARTITION BY policy_key, product_line_key) = 0
					THEN 2
				ELSE 1
			END priority_order_product_line,
			CASE
				WHEN SUM(IFNULL(billed_premium_amt_lcl, 0)) OVER (PARTITION BY policy_key, producer_key) = 0
				AND SUM(IFNULL(revenue_amt_lcl, 0)) OVER (PARTITION BY policy_key, producer_key) = 0
					THEN 2
				ELSE 1
			END priority_order_producer01,
			CASE
				WHEN SUM(IFNULL(billed_premium_amt_lcl, 0)) OVER (PARTITION BY policy_key, client_producer_key) = 0
				AND SUM(IFNULL(revenue_amt_lcl, 0)) OVER (PARTITION BY policy_key, client_producer_key) = 0
					THEN 2
				ELSE 1
			END priority_order_client_producer,
			CASE
				WHEN SUM(IFNULL(billed_premium_amt_lcl, 0)) OVER (PARTITION BY policy_key, client_account_manager_key) = 0
				AND SUM(IFNULL(revenue_amt_lcl, 0)) OVER (PARTITION BY policy_key, client_account_manager_key) = 0
					THEN 2
				ELSE 1
			END priority_order_client_account_manager,
			CASE
				WHEN SUM(IFNULL(billed_premium_amt_lcl, 0)) OVER (PARTITION BY policy_key, invoice_date_key) = 0
				AND SUM(IFNULL(revenue_amt_lcl, 0)) OVER (PARTITION BY policy_key, invoice_date_key) = 0
					THEN 2
				ELSE 1
			END priority_order_invoice_date,
			CASE
				WHEN SUM(IFNULL(billed_premium_amt_lcl, 0)) OVER (PARTITION BY policy_key, bill_type_key) = 0
				AND SUM(IFNULL(revenue_amt_lcl, 0)) OVER (PARTITION BY policy_key, bill_type_key) = 0
					THEN 2
				ELSE 1
			END priority_order_billing_type
		FROM {{ ref('revenue_fact_base') }}
-- This model replaces the first CTE (revenue_fact_base) in the stored procedure
SELECT 
			a.policy_key,
			a.bu_key,
			a.bu_department_key,
			a.bu_state_key,
			a.client_key,
			a.carrier_insurer_key,
			a.carrier_payee_key,
			a.product_key,
			a.product_line_key,
			a.producer_key,
			a.client_producer_key,
			a.client_account_manager_key,
			a.invoice_date_key,
			a.bill_type_key,
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
			CASE
				WHEN a.commission_revenue_amt_lcl <> 0
				AND a.commission_revenue_amt_lcl <> a.billed_premium_amt_lcl
					THEN a.billed_premium_amt_lcl
				ELSE 0
			END AS commission_premium_amt_lcl,
			CASE
				WHEN a.commission_revenue_amt_usd <> 0
				AND a.commission_revenue_amt_usd <> a.billed_premium_amt_usd
					THEN a.billed_premium_amt_usd
				ELSE 0
			END AS commission_premium_amt_usd,
			CASE
				WHEN a.commission_revenue_amt_pegusd <> 0
				AND a.commission_revenue_amt_pegusd <> a.billed_premium_amt_pegusd
					THEN a.billed_premium_amt_pegusd
				ELSE 0
			END AS commission_premium_amt_pegusd,
			CASE
				WHEN a.commission_revenue_amt_trns <> 0
				AND a.commission_revenue_amt_trns <> a.billed_premium_amt_trns
					THEN a.billed_premium_amt_trns
				ELSE 0
			END AS commission_premium_amt_trns,
			CASE
				WHEN a.commission_revenue_amt_lcl = 0
				OR a.commission_revenue_amt_lcl = a.billed_premium_amt_lcl
					THEN a.billed_premium_amt_lcl
				ELSE 0
			END AS fee_premium_amt_lcl,
			CASE
				WHEN a.commission_revenue_amt_usd = 0
				OR a.commission_revenue_amt_usd = a.billed_premium_amt_usd
					THEN a.billed_premium_amt_usd
				ELSE 0
			END AS fee_premium_amt_usd,
			CASE
				WHEN a.commission_revenue_amt_pegusd = 0
				OR a.commission_revenue_amt_pegusd = a.billed_premium_amt_pegusd
					THEN a.billed_premium_amt_pegusd
				ELSE 0
			END AS fee_premium_amt_pegusd,
			CASE
				WHEN a.commission_revenue_amt_trns = 0
				OR a.commission_revenue_amt_trns = a.billed_premium_amt_trns
					THEN a.billed_premium_amt_trns
				ELSE 0
			END AS fee_premium_amt_trns,
			a.commission_revenue_amt_usd + a.fee_revenue_amt_usd + a.brokerage_expense_amt_usd + a.agent_commission_amt_usd AS revenue_amt_lcl,
			CASE
				WHEN IFNULL(b.carrier_master_parent_name, '') ILIKE 'Gallagher Global Brokerage-US'
					THEN 0
				WHEN UPPER(d.region_name) ILIKE 'GGB ANZ - Broking NZ'
				AND
				(
					c.client_id ILIKE 'MNZ%'
				OR  b.carrier_name ILIKE 'Certain Underwriters at Lloyd''s (B1262BW0127720)'
				)
					THEN 0
				ELSE 1
			END premium_amt_factor

FROM {{ ref('edw_f_revenue_detail') }} a

INNER JOIN {{ ref('edw_d_carrier') }} b
  ON a.carrier_payee_key = b.carrier_key
INNER JOIN {{ ref('edw_d_client') }} c
  ON a.client_key = c.client_key
INNER JOIN {{ ref('edw_d_bu') }} d
  ON a.bu_key = d.bu_key

where a.ENV_SOURCE_CODE = 'FDW'
OR a.env_source_code = 'some other variable'
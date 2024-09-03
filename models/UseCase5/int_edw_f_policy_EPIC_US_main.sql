
SELECT 
a.policy_key,
		a.client_key,
		a.carrier_insurer_key,
		a.carrier_payee_key,
		a.product_key,
		a.product_line_key,
		a.producer_01_key,
		a.producer_02_key,
a.account_manager_key,
		a.effective_date_key,
		a.expiration_date_key,
		a.inception_date_key,
		a.cancel_date_key,
		a.reinstated_date_key,
		a.invoice_date_key,
		a.contracted_expiration_date_key,
		a.client_producer_key,
		a.client_account_manager_key,
		a.bu_key,
		a.bu_department_key,
		a.bu_state_key,
		-- IFNULL(b.renewal_policy_key, :env_variables['unknown_key']) AS renewal_policy_key,
		-- IFNULL(c.prior_market_key, :env_variables['unknown_key']) AS prior_market_key,
		-- IFNULL(c.future_market_key, :env_variables['unknown_key']) AS future_market_key,
		b.renewal_policy_key AS renewal_policy_key,
		c.prior_market_key AS prior_market_key,
		c.future_market_key AS future_market_key,
		a.renewal_date_key,
		a.financing_company_key,
		a.excess_policy_key,
		a.source_system_key,
		a.source_system_instance_key,
		a.bill_type_key,
		-- IFNULL(c.active_policy_status_key, :env_variables['unknown_key']) AS active_policy_status_key,
		-- IFNULL(c.ajg_new_client_status_key, :env_variables['unknown_key']) AS ajg_new_client_status_key,
		-- IFNULL(c.ajg_lost_business_detail_key, :env_variables['unknown_key']) AS ajg_lost_business_detail_key,
		-- IFNULL(c.ajg_lost_client_status_key, :env_variables['unknown_key']) AS ajg_lost_client_status_key,
		-- IFNULL(c.ajg_new_business_detail_key, :env_variables['unknown_key']) AS ajg_new_business_detail_key,
		-- IFNULL(c.fee_converted_lost_detail_key, :env_variables['unknown_key']) AS fee_converted_lost_detail_key,
		-- IFNULL(c.fee_converted_new_detail_key, :env_variables['unknown_key']) AS fee_converted_new_detail_key,
		-- IFNULL(c.fee_split_lost_detail_key, :env_variables['unknown_key']) AS fee_split_lost_detail_key,
		-- IFNULL(c.fee_split_new_detail_key, :env_variables['unknown_key']) AS fee_split_new_detail_key,
		-- IFNULL(c.future_market_type_key, :env_variables['unknown_key']) AS future_market_type_key,
		-- IFNULL(c.market_status_key, :env_variables['unknown_key']) AS market_status_key,
		-- IFNULL(c.prior_market_type_key, :env_variables['unknown_key']) AS prior_market_type_key,
		-- IFNULL(c.package_converted_lost_detail_key, :env_variables['unknown_key']) AS package_converted_lost_detail_key,
		-- IFNULL(c.package_converted_new_detail_key, :env_variables['unknown_key']) AS package_converted_new_detail_key,
		-- IFNULL(c.policy_occurrence_key, :env_variables['unknown_key']) AS policy_occurrence_key,
		c.active_policy_status_key AS active_policy_status_key,
		c.ajg_new_client_status_key AS ajg_new_client_status_key,
		c.ajg_lost_business_detail_key AS ajg_lost_business_detail_key,
		c.ajg_lost_client_status_key AS ajg_lost_client_status_key,
		c.ajg_new_business_detail_key AS ajg_new_business_detail_key,
		c.fee_converted_lost_detail_key AS fee_converted_lost_detail_key,
		c.fee_converted_new_detail_key AS fee_converted_new_detail_key,
		c.fee_split_lost_detail_key AS fee_split_lost_detail_key,
		c.fee_split_new_detail_key AS fee_split_new_detail_key,
		c.future_market_type_key AS future_market_type_key,
		c.market_status_key AS market_status_key,
		c.prior_market_type_key AS prior_market_type_key,
		c.package_converted_lost_detail_key AS package_converted_lost_detail_key,
		c.package_converted_new_detail_key AS package_converted_new_detail_key,
		c.policy_occurrence_key AS policy_occurrence_key,
		a.env_source_code,
		a.data_source_code,
		a.annualized_endorsement_premium_amt,
		a.written_premium_amt,
		a.annualized_premium_amt,
		a.estimated_premium_amt,
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
		a.contracted_expiration_date,
		c.active_policy_status_code,
		c.ajg_new_client_status_code,
		c.ajg_lost_business_detail_code,
		c.ajg_lost_client_status_code,
		c.ajg_new_business_detail_code,
		--c.fee_converted_lost_detail_code,
		--c.fee_converted_new_detail_code,
		--c.fee_split_lost_detail_code,
		--c.fee_split_new_detail_code,
		c.future_market_type_code,
		c.market_status_code,
		c.prior_market_type_code,
		-- c.package_converted_lost_detail_code,
		-- c.package_converted_new_detail_code,
		c.policy_occurrence_code
FROM {{ ref('int_edw_f_policy_EPIC_US_base') }} AS a
LEFT OUTER JOIN {{ ref('edw_f_policy_renewal_final') }} AS b
  ON b.policy_key = a.policy_key
LEFT OUTER JOIN {{ ref('policy_lifecycle_attributes_final') }} AS c
  ON c.policy_key = a.policy_key
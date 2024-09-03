WITH policy AS (
  SELECT 
    pb.bu_key,
    pb.product_key,
    pb.product_line_key,
    pb.client_key,
    dp.policy_key,
    dp.policy_id,
    dp.original_policy_id,
    CASE
      WHEN rc.policy_renewal_config_code IS NULL
        THEN dp.policy_num
      ELSE rc.column_value_to
    END AS _policy_num,
    CASE
      WHEN TRY_TO_NUMBER(_policy_num) IS NULL
        THEN _policy_num
      WHEN REGEXP_LIKE(_policy_num, '^0+$')
        THEN '0'
      ELSE LTRIM(_policy_num, '0')
    END AS policy_num,
    dp.effective_date,
    dp.expiration_date
  FROM {{ ref('renewal_policy_main') }} AS pb
INNER JOIN (
  SELECT policy_key
  FROM {{ ref('edw_f_revenue_detail') }}
  WHERE env_source_code = 'FDW'  
  GROUP BY policy_key
  HAVING
    SUM(billed_premium_amt_lcl) <> 0
    OR SUM(commission_revenue_amt_lcl) <> 0
    OR SUM(fee_revenue_amt_lcl) <> 0
) AS r ON pb.policy_key = r.policy_key
  INNER JOIN {{ ref('edw_f_policy') }} AS dp ON pb.policy_key = dp.policy_key
  LEFT OUTER JOIN {{ ref('mdm_s_policy_renewal_config') }} AS rc
  ON rc.source_system_code = dp.env_source_code
			AND LOWER(rc.column_name) IN ('PolicyNum', 'policy_num')
			AND dp.policy_num = IFNULL(rc.column_value_from, '')
)
SELECT
		pb.policy_key,
		bu.division_code,
        pb.policy_id,
        pb.original_policy_id,
        pb.policy_num,
		CASE
			WHEN NOT cl.exposed_ind
				THEN cl.client_name
			ELSE
				COALESCE
				(
					o.global_ultimate_business_name,
					o.domestic_ultimate_business_name,
					(
						CASE
							WHEN IFNULL(cl.client_master_name, '') = ''
							OR UPPER(cl.client_master_name) = 'UNKNOWN'
								THEN IFNULL(cl.client_name, 'Unspecified')
							ELSE cl.client_master_name
						END
					)
				)
		END AS client_name,
		pm.product_master_line_name,
		pb.effective_date,
		pb.expiration_date,
		CASE WHEN LOWER(IFNULL(client_name, 'unknown')) IN ('unknown', 'no match', 'invalid') THEN FALSE ELSE TRUE END AS valid_client_name_ind,
		CASE WHEN LOWER(IFNULL(pm.product_master_line_name, 'unknown')) IN ('unknown', 'no match', 'invalid') THEN FALSE ELSE TRUE END AS valid_product_master_line_name_ind,
        LAG(pb.expiration_date) OVER
									(
										PARTITION BY
											UPPER(cl.client_name),
											UPPER(pm.product_master_line_name),
											bu.division_code
										ORDER BY
											pb.effective_date,
											pb.policy_key
									) AS expiration_date_prev,
        COUNT(*) OVER (PARTITION BY bu.division_code, pb.original_policy_id) AS original_policy_id_count,
        COUNT(*) OVER (PARTITION BY UPPER(cl.client_name), bu.division_code, UPPER(pb.policy_num)) AS policy_num_count,
        COUNT(*) OVER (PARTITION BY UPPER(cl.client_name), UPPER(pm.product_master_line_name), bu.division_code) AS client_product_count
	FROM
		policy AS pb
		INNER JOIN
		{{ ref('edw_d_bu') }} AS bu
		ON bu.bu_key = pb.bu_key
		INNER JOIN
		{{ ref('edw_d_product') }} AS pr
		ON pr.product_key =
			(
				CASE
					--WHEN pb.product_key > :env_variables['unknown_key'] THEN pb.product_key
                    WHEN pb.product_key > 0 THEN pb.product_key
					ELSE pb.product_line_key
				END
			)
		INNER JOIN
		{{ ref('edw_d_product_master') }} AS pm
		ON pm.product_master_key = pr.product_master_key
		INNER JOIN
		{{ ref('edw_d_client') }} AS cl
		ON pb.client_key = cl.client_key
		INNER JOIN
		{{ ref('edw_d_organization') }} AS o
		ON cl.organization_key = o.organization_key
Select * from {{ ref('stg_s_dim_policy') }} as a
inner JOIN
{{ ref('stg_s_dim_extract') }} as b
ON a.extract_key = b.extract_key
		WHERE a.office_agency_system_key = 2
	    AND IFNULL(TRIM(a.policy_id), '') <> ''	
		QUALIFY ROW_NUMBER() OVER 
								(
									PARTITION BY
										TRIM(a.policy_id),
										a.office_agency_system_key
									ORDER BY
										a.extract_key DESC,
										a.policy_key DESC
								) = 1
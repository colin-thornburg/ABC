WITH
	policy_renewal_main_1 AS
	(
        SELECT
            policy_key,
            renewal_policy_key,
            original_policy_id,
            effective_date,
            division_code,
            COUNT(*) OVER(PARTITION BY renewal_policy_key) AS renewal_policy_key_count
        FROM {{ ref('edw_f_policy_renewal_main_1') }}
    )
    SELECT
        policy_key,
        CASE
            WHEN original_policy_id IS NULL
                THEN renewal_policy_key
            ELSE FIRST_VALUE(renewal_policy_key) OVER
													(
														PARTITION BY
															original_policy_id,
															division_code
														ORDER BY
															effective_date,
															renewal_policy_key_count DESC,
															policy_key
													)
		END AS renewal_policy_key
    FROM policy_renewal_main_1
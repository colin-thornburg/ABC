-- This model replaces the third CTE in the stored procedure
WITH policy_renewal_base_1 AS (
  SELECT policy_key,
			original_policy_id,
			effective_date,
			division_code,
			FIRST_VALUE(policy_key) OVER
										(
											PARTITION BY
												division_code,
												original_policy_id
											ORDER BY
												effective_date,
												policy_key
										) AS renewal_policy_key
  FROM {{ ref('edw_f_policy_renewal_base') }}
  WHERE original_policy_id_count > 1
  AND original_policy_id IS NOT NULL
),
policy_renewal_base_2 AS (
  SELECT policy_key,
			original_policy_id,
			effective_date,
			division_code,
			FIRST_VALUE(policy_key) OVER
										(
											PARTITION BY
												UPPER(client_name),
												division_code,
												UPPER(policy_num)
											ORDER BY
												effective_date,
												policy_key
										) AS renewal_policy_key
  FROM {{ ref('edw_f_policy_renewal_base') }}
  WHERE policy_num_count > 1
  AND policy_num IS NOT NULL
),
policy_renewal_base_3 AS (
  SELECT
			policy_key,
			original_policy_id,
			effective_date,
			division_code,
			FIRST_VALUE(policy_key) OVER
										(
											PARTITION BY
												UPPER(client_name),
												product_master_line_name,
												division_code,
												group_num
											ORDER BY
												effective_date,
												policy_key
										) AS renewal_policy_key
		FROM
		(
			SELECT
				policy_key,
				division_code,
				original_policy_id,
				client_name,
				product_master_line_name,
				effective_date,
				SUM
				(
					CASE
						WHEN effective_date = expiration_date_prev
							THEN 0
						ELSE 1
					END
				) OVER
					(
						PARTITION BY
							UPPER(client_name),
							UPPER(product_master_line_name),
							division_code
						ORDER BY
							effective_date,
							policy_key
					) AS group_num
			FROM
			(
				SELECT
					policy_key,
					division_code,
					original_policy_id,
					client_name,
					product_master_line_name,
					effective_date,
					expiration_date_prev,
					client_product_count,
                    valid_client_name_ind,
                    valid_product_master_line_name_ind,
					SUM
					(
						CASE
							WHEN effective_date = expiration_date_prev
								THEN 1
							ELSE 0
						END
					) OVER
						(
							PARTITION BY
								UPPER(client_name),
								UPPER(product_master_line_name),
								division_code
						) + 1 AS uncrossed_date_range_count
				FROM {{ ref('edw_f_policy_renewal_base') }}
			)
			WHERE valid_client_name_ind
			AND valid_product_master_line_name_ind
			AND client_product_count > 1
			AND client_product_count = uncrossed_date_range_count
			AND original_policy_id IS NOT NULL
		)
	),
policy_renewal_base_4 AS (
  SELECT
			policy_key,
			original_policy_id,
			effective_date,
			division_code,
			FIRST_VALUE(policy_key) OVER
										(
											PARTITION BY
												UPPER(client_name),
												UPPER(product_master_line_name),
												division_code,
												group_num
											ORDER BY
												effective_date,
												policy_key
										) AS renewal_policy_key
		FROM
		(
			SELECT
				policy_key,
				division_code,
				original_policy_id,
				client_name,
				product_master_line_name,
				effective_date,
				SUM
				(
					CASE
						WHEN effective_date = expiration_date_prev
							THEN 0
						ELSE 1
					END
				) OVER
					(
						PARTITION BY
							UPPER(client_name),
							UPPER(product_master_line_name),
							division_code
						ORDER BY
							effective_date,
							policy_key
					) AS group_num
			FROM {{ ref('edw_f_policy_renewal_base') }}
			WHERE valid_client_name_ind
			AND valid_product_master_line_name_ind
			AND (original_policy_id_count <= 1 OR original_policy_id IS NULL)
			AND policy_num_count <= 1
		))
SELECT *
FROM (
  SELECT policy_key,
			renewal_policy_key,
			original_policy_id,
			effective_date,
			division_code,
			COUNT(*) OVER(PARTITION BY renewal_policy_key) AS renewal_policy_key_count FROM policy_renewal_base_1
  UNION ALL
  SELECT policy_key,
			renewal_policy_key,
			original_policy_id,
			effective_date,
			division_code,
			COUNT(*) OVER(PARTITION BY renewal_policy_key) AS renewal_policy_key_count FROM policy_renewal_base_2
  UNION ALL
  SELECT policy_key,
			renewal_policy_key,
			original_policy_id,
			effective_date,
			division_code,
			COUNT(*) OVER(PARTITION BY renewal_policy_key) AS renewal_policy_key_count FROM policy_renewal_base_3
  UNION ALL
  SELECT policy_key,
			renewal_policy_key,
			original_policy_id,
			effective_date,
			division_code,
			COUNT(*) OVER(PARTITION BY renewal_policy_key) AS renewal_policy_key_count FROM policy_renewal_base_4
)
QUALIFY(ROW_NUMBER() OVER(PARTITION BY policy_key ORDER BY renewal_policy_key_count DESC, renewal_policy_key)) = 1
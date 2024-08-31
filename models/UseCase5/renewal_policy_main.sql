-- This model replaces the first CTE in the stored procedure
-- It combines data from temp_edw_f_policy_xxx_base and edw.f_policy
WITH policy_main AS (
  SELECT policy_key,
            bu_key,
            product_key,
            product_line_key,
            client_key,
            FALSE AS source_system_purge_ind 
  FROM {{ ref('int_edw_f_policy_EPIC_US_base') }}
  UNION ALL
  SELECT policy_key,
            bu_key,
            product_key,
            product_line_key,
            client_key,
            source_system_purge_ind 
  FROM {{ ref('edw_f_policy') }}
  WHERE env_source_code = 'FDW'
  AND source_system_purge_ind
  UNION ALL
  SELECT policy_key,
            bu_key,
            product_key,
            product_line_key,
            client_key,
            source_system_purge_ind 
  FROM {{ ref('edw_f_policy') }}
  WHERE env_source_code IN ('EPIC_US', 'EPIC_CAN', 'AIM_CF', 'FDW')
)
SELECT policy_key,
        bu_key,
        product_key,
        product_line_key,
        client_key
FROM policy_main
QUALIFY ROW_NUMBER() OVER(PARTITION BY policy_key ORDER BY source_system_purge_ind) = 1
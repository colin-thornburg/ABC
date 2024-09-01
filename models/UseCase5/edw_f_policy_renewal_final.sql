-- This model replaces the final CTE in the stored procedure
WITH policy_all AS (
  SELECT policy_key,
            renewal_policy_key
  FROM {{ ref('edw_f_policy_renewal_main_2') }}
  UNION ALL
  SELECT policy_key,
            policy_key AS renewal_policy_key
  FROM {{ ref('edw_f_policy_renewal_base') }}
  WHERE policy_key NOT IN (
    SELECT policy_key
    FROM {{ ref('edw_f_policy_renewal_main_2') }}
  )
)
SELECT a.policy_key,
        IFNULL(b.renewal_policy_key, a.renewal_policy_key) AS renewal_policy_key
FROM policy_all AS a
LEFT OUTER JOIN {{ ref('edw_f_policy_renewal_override') }} AS b
  ON a.policy_key = b.policy_key
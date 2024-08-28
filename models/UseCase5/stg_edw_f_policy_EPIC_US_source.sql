-- This model replaces the first CTE (policy_base) and subsequent CTEs in the stored procedure
WITH policy_base AS (
  SELECT *
  --FROM { source('os1_fdw', 's_dim_policy') }} AS a
    FROM {{ ref('fdw_s_dim_policy') }} AS a
  --INNER JOIN { source('os1_fdw', 's_dim_extract') }} AS b
  INNER JOIN {{ ref('fdw_s_dim_extract') }} AS b
    ON a.extract_key = b.extract_key
  WHERE a.office_agency_system_key = 2
  AND IFNULL(TRIM(a.agency_system_policy_id), '') <> ''	
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY
      TRIM(a.agency_system_policy_id),
      a.office_agency_system_key
    ORDER BY
      a.extract_key DESC,
      a.policy_key DESC
  ) = 1
),
policy_bu AS (
  -- Logic for policy_bu CTE
),
employee AS (
  -- Logic for employee CTE
),
client AS (
  -- Logic for client CTE
),
policy_main AS (
  -- Logic for policy_main CTE
)
SELECT *
FROM policy_main AS a
-- LEFT OUTER JOINs with edw tables as in the original stored procedure
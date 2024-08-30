-- This model replaces the temp_edw_f_policy_EPIC_US_base table in the stored procedure
SELECT *
FROM {{ ref('stg_edw_f_policy_EPIC_US_source') }} AS a
--LEFT OUTER JOIN { ref('temp_edw_f_policy_EPIC_US_revenue_detail') }} AS b
--  ON b.policy_key = a.policy_key
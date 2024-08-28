-- This model replaces the first CTE (revenue_fact_base) in the stored procedure
SELECT *
--FROM {{ source('edw', 'f_revenue_detail') }} a
FROM {{ ref('edw_f_revenue_detail') }} a
--INNER JOIN {{ source('edw', 'd_carrier') }} b
INNER JOIN {{ ref('edw_d_carrier') }} b
  ON a.carrier_payee_key = b.carrier_key
INNER JOIN {{ ref('edw_d_client') }} c
  ON a.client_key = c.client_key
INNER JOIN {{ ref('edw_d_bu') }} d
  ON a.bu_key = d.bu_key
WHERE a.env_source_code = '{{ var("env_source_code") }}'
OR a.env_source_code = CASE
  WHEN '{{ var("env_source_code") }}' IN ('EPIC_US', 'EPIC_CAN')
    THEN 'FDW'
  ELSE NULL
END
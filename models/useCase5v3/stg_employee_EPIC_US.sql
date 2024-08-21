{{ config(materialized='ephemeral') }}

SELECT
    a.employee_key,
    TRIM(a.agency_system_employee_code) AS agency_system_employee_code,
    IFNULL(TRIM(a.employee_name), '') AS employee_name
-- FROM { source('os1_fdw', 's_dim_employee') }} AS a
FROM {{ ref('s_dim_employee') }} AS a
-- INNER JOIN { source('os1_fdw', 's_dim_extract') }} AS b
INNER JOIN {{ ref('s_dim_extract') }} AS b
    ON a.extract_key = b.extract_key
WHERE a.employee_key != 0
  AND UPPER(IFNULL(TRIM(a.employee_name), '')) NOT IN ('{ NO EMPLOYEE }', '')
  AND IFNULL(TRIM(a.agency_system_employee_code), '') != ''
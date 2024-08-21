-- models/staging/stg_client_EPIC_US.sql

{{ config(materialized='ephemeral') }}

SELECT
    TRIM(a.agency_system_client_id) AS client_id,
    a.producer1_employee_key,
    a.csr1_employee_key,
    a.client_key,
    IFNULL(TRIM(a.agency_system_client_code), '') AS agency_system_client_code,
    IFNULL(TRIM(a.client_name), '') AS client_name
-- FROM { source('os1_fdw', 's_dim_client') }} AS a
FROM {{ ref('s_dim_client') }} AS a
-- INNER JOIN { source('os1_fdw', 's_dim_extract') }} AS b
INNER JOIN {{ ref('s_dim_extract') }} AS b
    ON a.extract_key = b.extract_key
QUALIFY ROW_NUMBER() OVER (PARTITION BY a.client_key ORDER BY b.extract_key DESC) = 1
-- models/staging/stg_policy_EPIC_US_bu.sql

{{ config(materialized='ephemeral') }}

SELECT
    p.policy_key,
    c.bu_id,
    TRIM(d.department_code) AS department_code
FROM {{ ref('stg_policy_EPIC_US_source') }} AS p
--LEFT OUTER JOIN { source('os1_fdw', 's_dim_department') }} AS d
LEFT OUTER JOIN {{ ref('s_dim_department') }} AS d
    ON p.department_key = d.department_key
-- LEFT OUTER JOIN { source('mdm', 's_department_to_bu_xref') }} AS c
LEFT OUTER JOIN {{ ref('s_department_to_bu_xref') }} AS c
    ON TRIM(d.department_code) = TRIM(c.department_code)
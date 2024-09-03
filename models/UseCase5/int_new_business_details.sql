{{ config(materialized='view') }}

WITH policy_base AS (
    SELECT * FROM {{ ref('seed_policy_base') }}
)

SELECT
    policy_key,
    CASE
        WHEN policy_key = 1 THEN 'New Client'
        WHEN policy_key = 2 THEN 'New Product'
        WHEN policy_key = 5 THEN 'New Policy'
        ELSE 'Not Applicable'
    END AS ajg_new_business_detail_code,
    CASE
        WHEN policy_key IN (1, 2, 5) THEN 'New'
        ELSE 'Renewal'
    END AS ajg_new_client_status_code
FROM policy_base
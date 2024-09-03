{{ config(materialized='view') }}

WITH policy_base AS (
    SELECT * FROM {{ ref('seed_policy_base') }}
)

SELECT
    policy_key,
    CASE
        WHEN policy_key = 3 THEN 'Lost Client'
        WHEN policy_key = 4 THEN 'Lost Product'
        ELSE 'Not Applicable'
    END AS ajg_lost_business_detail_code,
    CASE
        WHEN policy_key IN (3, 4) THEN 'Lost'
        ELSE 'Renewal'
    END AS ajg_lost_client_status_code
FROM policy_base
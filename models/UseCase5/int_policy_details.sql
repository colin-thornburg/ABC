{{ config(materialized='view') }}

WITH policy_base AS (
    SELECT * FROM {{ ref('seed_policy_base') }}
)

SELECT
    policy_key,
    CASE
        WHEN policy_start_date > CURRENT_DATE() THEN 'Future Policy'
        WHEN policy_start_date <= CURRENT_DATE() AND policy_end_date > CURRENT_DATE() THEN 'Active Policy'
        ELSE 'Expired Policy'
    END AS active_policy_status_code,
    policy_start_date,
    policy_end_date
FROM policy_base
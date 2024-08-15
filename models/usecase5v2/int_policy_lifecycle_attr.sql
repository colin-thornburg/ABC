{{
    config(
        materialized='view'
    )
}}

WITH policy_lifecycle_base AS (
    SELECT
        policy_key,
        client_key,
        bu_id,
        effective_date,
        expiration_date,
        CASE
            WHEN expiration_date > CURRENT_DATE() THEN 'Active Policy'
            ELSE 'Inactive Policy'
        END AS policy_status
    FROM {{ ref('int_policy_bu') }}
)
SELECT 
    policy_key,
    client_key,
    bu_id,
    effective_date,
    expiration_date,
    policy_status,
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM policy_lifecycle_base
{{
    config(
        materialized='view'
    )
}}

WITH policy_renewal_base AS (
    SELECT
        p.policy_key,
        p.policy_id,
        CASE
            WHEN rc.policy_renewal_config_code IS NULL THEN p.policy_num
            ELSE rc.column_value_to
        END AS renewal_policy_num
    FROM {{ ref('stg_os1_fdw__s_dim_policy') }} p
    LEFT JOIN {{ ref('stg_edw__s_policy_renewal_config') }} rc
        ON rc.source_system_code = p.env_source_code
    WHERE p.env_source_code = 'EPIC_US'
    --WHERE p.env_source_code = ' var("env_source_code_EPIC_US") }}'

)
SELECT 
    policy_key,
    policy_id,
    policy_key AS renewal_policy_key,
    renewal_policy_num,
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM policy_renewal_base
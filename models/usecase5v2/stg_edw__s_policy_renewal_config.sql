{{ config(materialized='view') }}

SELECT
    source_system_code,
    policy_renewal_config_code,
    column_value_to,
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM {{ ref('s_policy_renewal_config') }}
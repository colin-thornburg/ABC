{{ config(materialized='view') }}

SELECT
    policy_key,
    policy_id,
    policy_num,
    env_source_code,
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM {{ ref('d_policy_UC5') }}
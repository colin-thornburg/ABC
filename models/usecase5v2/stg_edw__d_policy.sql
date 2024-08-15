{{ config(materialized='view') }}

SELECT
    policy_key,
    policy_id,
    policy_num,
    NULLIF(TO_DATE(effective_date), '1900-01-01 00:00:00.000') AS effective_date,
    NULLIF(TO_DATE(expiration_date), '1900-01-01 00:00:00.000') AS expiration_date,
    NULLIF(TO_DATE(inception_date), '1900-01-01 00:00:00.000') AS inception_date,
    env_source_code,
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM {{ ref('d_policy_UC5') }}
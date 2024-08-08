{{ config(materialized='table') }}

WITH source_data AS (
    SELECT * FROM {{ ref('int_user_access_expanded') }}
),
fact_data AS (
    SELECT * FROM {{ ref('fct_user_access') }}
)
SELECT
    sd.user_access_group_id,
    sd.bu_key,
    sd.source_system,
    sd.user_id,
    sd.role_based_code,
    sd.access_type,
    'Missing in fact table' AS error_description,
    current_timestamp() AS etl_updated_at
FROM source_data sd
LEFT JOIN fact_data fd
    ON sd.user_access_group_id = fd.user_access_group_id
    AND sd.bu_key = fd.bu_key
    AND sd.source_system = fd.source_system
WHERE fd.user_access_group_id IS NULL
WITH source AS (
    SELECT * FROM {{ ref('s_dim_epic_policy_line_type') }}
),

renamed AS (
    SELECT
        epic_policy_line_type_key::VARCHAR AS epic_policy_line_type_key,
        policy_line_type_code::VARCHAR AS policy_line_type_code,
        policy_line_type_description::VARCHAR AS policy_line_type_description
    FROM source
)

SELECT 
    *,
    CURRENT_TIMESTAMP() AS dbt_loaded_at,
    'EPIC_US' AS data_source
FROM renamed
WITH source AS (
    SELECT * FROM {{ ref('s_dim_line') }}
),

renamed AS (
    SELECT
        line_key::VARCHAR AS line_key,
        policy_key::INTEGER AS policy_key,
        epic_program_key::VARCHAR AS epic_program_key,
        agency_system_policy_type::VARCHAR AS agency_system_policy_type
    FROM source
)

SELECT 
    *,
    CURRENT_TIMESTAMP() AS dbt_loaded_at,
    'EPIC_US' AS data_source
FROM renamed
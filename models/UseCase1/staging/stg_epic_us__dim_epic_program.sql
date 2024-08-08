WITH source AS (
    SELECT * FROM {{ ref('s_dim_epic_program') }}
),

renamed AS (
    SELECT
        epic_program_key::VARCHAR AS epic_program_key,
        program::VARCHAR AS program,
        product::VARCHAR AS product
    FROM source
)

SELECT 
    *,
    CURRENT_TIMESTAMP() AS dbt_loaded_at,
    'EPIC_US' AS data_source
FROM renamed
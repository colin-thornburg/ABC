WITH source AS (
    SELECT * FROM {{ ref('s_policy_program_input') }}
),

renamed AS (
    SELECT
        policy_id::VARCHAR AS policy_id,
        program_eligibility_code::VARCHAR AS program_eligibility_code,
        program_eligibility_name::VARCHAR AS program_eligibility_name,
        program_desc_01::VARCHAR AS program_desc_01,
        program_desc_02::VARCHAR AS program_desc_02
    FROM source
)

SELECT 
    *,
    CURRENT_TIMESTAMP() AS dbt_loaded_at,
    'MDM' AS data_source
FROM renamed
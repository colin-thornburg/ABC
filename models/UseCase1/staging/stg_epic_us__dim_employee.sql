WITH source AS (
    SELECT * FROM {{ ref('s_dim_employee') }}
),

renamed AS (
    SELECT
        employee_key::VARCHAR AS employee_key,
        employee_name::VARCHAR AS employee_name,
        employee_role::VARCHAR AS employee_role
    FROM source
)

SELECT 
    *,
    CURRENT_TIMESTAMP() AS dbt_loaded_at,
    'EPIC_US' AS data_source
FROM renamed
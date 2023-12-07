-- should this be incremental?

{{
    config(
        materialized='table',
        schema='dbo',
        database='colint_dev'
    )
}}

-- final_patient.sql

WITH final_patient_data AS (
    SELECT
        *
        -- Any additional transformations needed for the final patient data
    FROM {{ ref('separated_address') }} -- Reference the 'separated_address' model
)
SELECT
    *
    -- Additional transformations if needed
FROM final_patient_data

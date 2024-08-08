WITH source AS (
    SELECT * FROM {{ ref('s_dim_policy') }}
),

renamed AS (
    SELECT
        policy_key::INTEGER AS policy_key,
        agency_system_policy_id::VARCHAR AS policy_id,
        policy_number::VARCHAR AS policy_number,
        TRY_TO_DATE(effective_date) AS effective_date,
        TRY_TO_DATE(expiration_date) AS expiration_date,
        TRY_TO_DATE(inception_date) AS inception_date,
        policy_status::VARCHAR AS policy_status,
        bill_type::VARCHAR AS bill_type,
        written_premium::FLOAT AS written_premium,
        annualized_premium::FLOAT AS annualized_premium,
        producer1_employee_key::VARCHAR AS producer_employee_key,
        csr1_employee_key::VARCHAR AS csr_employee_key,
        is_active::BOOLEAN AS is_active,
        agency_system_original_policy_id::VARCHAR AS original_policy_id,
        office_agency_system_key::VARCHAR AS office_agency_system_key,
        extract_key::INTEGER AS extract_key,
        is_deleted::BOOLEAN AS is_deleted,
        TRY_TO_DATE(contracted_expiration_date) AS contracted_expiration_date,
        cancellation_reason::VARCHAR AS cancellation_reason,
        cancellation_reason_other::VARCHAR AS cancellation_reason_other
    FROM source
)

SELECT 
    *,
    CURRENT_TIMESTAMP() AS dbt_loaded_at,
    'EPIC_US' AS data_source
FROM renamed
WITH source AS (
    SELECT * FROM {{ ref('policy') }}
),
renamed AS (
    SELECT
        policy_key,
        agency_system_policy_id AS policy_id,
        policy_number AS policy_num,
        effective_date,
        expiration_date,
        inception_date,
        policy_status AS policy_status_code,
        bill_type AS bill_type_code,
        written_premium AS written_premium_amt,
        annualized_premium AS annualized_premium_amt,
        producer1_employee_key AS producer_source_sid,
        csr1_employee_key AS account_manager_source_sid,
        is_active AS active_ind,
        agency_system_original_policy_id AS original_policy_id,
        office_agency_system_key,
        extract_key
    FROM source
)
SELECT *
FROM renamed
WHERE office_agency_system_key = 'OAS001'  -- This can be a variable and dynamic if necessary
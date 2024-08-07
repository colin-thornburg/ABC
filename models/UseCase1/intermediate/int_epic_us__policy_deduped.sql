-- models/intermediate/int_epic_us__policy_deduped.sql

WITH policy_data AS (
    SELECT
        p.*,
        pp.program,
        pp.product AS policy_level_source_code
    FROM {{ ref('stg_epic_us__policy') }} p
    LEFT JOIN {{ ref('stg_epic_us__policy_product') }} pp
    ON p.policy_key = pp.policy_key
),

deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY policy_id, office_agency_system_key
            ORDER BY extract_key DESC, policy_key DESC
        ) AS row_num
    FROM policy_data
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['policy_id', 'office_agency_system_key']) }} AS policy_sk,
    'EPIC_US' AS env_source_code,
    'Policy' AS data_source_code,
    policy_id,
    policy_key AS policy_source_sid,
    policy_num,
    effective_date,
    expiration_date,
    inception_date,
    policy_status_code,
    policy_status_code AS policy_status_desc, -- Assuming the code and description are the same for this source
    bill_type_code,
    written_premium_amt,
    annualized_premium_amt,
    producer_source_sid,
    account_manager_source_sid,
    active_ind,
    policy_level_source_code,
    original_policy_id,
    program AS program_desc_01,
    office_agency_system_key,
    CASE 
        WHEN DATEDIFF(day, effective_date, expiration_date) > 400 THEN TRUE 
        ELSE FALSE 
    END AS multi_year_ind,
    CURRENT_TIMESTAMP() AS etl_update_datetime,
    'ETL Process Name' AS etl_process_name, -- Replace with actual process name if available
    TRUE AS etl_active_ind -- All records are considered active at this stage
FROM deduped
WHERE row_num = 1
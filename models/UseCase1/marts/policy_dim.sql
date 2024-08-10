{{ config(
    materialized='incremental',
    unique_key=['env_source_code', 'data_source_code', 'policy_id'],
    on_schema_change='sync_all_columns'
) }}
/* Only update a subset of columns
merge_update_columns=[
        'etl_active_ind',
        'policy_source_sid',
        'policy_num',
        'effective_date',
        'expiration_date', ...]
*/
WITH source_data AS (
    SELECT * FROM {{ ref('int_epic_us__policy_combined') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['policy_id', 'office_agency_system_key']) }} AS policy_sk,
    'EPIC_US' AS env_source_code,
    'Policy' AS data_source_code,
    policy_id,
    policy_key AS policy_source_sid,
    policy_number AS policy_num,
    effective_date,
    expiration_date,
    inception_date,
    policy_status AS policy_status_code,
    policy_status AS policy_status_desc,
    bill_type AS bill_type_code,
    written_premium AS written_premium_amt,
    annualized_premium AS annualized_premium_amt,
    producer_employee_key AS producer_source_sid,
    producer_name,
    csr_employee_key AS account_manager_source_sid,
    account_manager_name,
    is_active AS active_ind,
    final_product AS policy_level_source_code,
    original_policy_id,
    final_program AS program_desc_01,
    program_desc_02,
    multi_year_ind,
    contracted_expiration_date,
    cancellation_reason,
    cancellation_reason_other,
    program_eligibility_code,
    program_eligibility_name,
    policy_line_type_code,
    policy_line_type_description,
    CASE 
        WHEN policy_key IS NOT NULL THEN TRUE 
        ELSE FALSE 
    END AS etl_active_ind,
    dbt_updated_at AS etl_update_datetime,
    'dbt_EPIC_US_policy' AS etl_process_name

FROM source_data

{% if is_incremental() %}

WHERE dbt_updated_at > (SELECT MAX(etl_update_datetime) FROM {{ this }})

{% endif %}
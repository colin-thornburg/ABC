-- models/mart/fct_epic_us__policy.sql

{{ 
    config(
        materialized='incremental',
        unique_key=['env_source_code', 'data_source_code', 'policy_id'],
        on_schema_change='sync_all_columns'
    )
}}

WITH source_data AS (
    SELECT * FROM {{ ref('int_epic_us__policy_deduped') }}
)

SELECT
    policy_sk,
    env_source_code,
    data_source_code,
    policy_id,
    policy_source_sid,
    policy_num,
    effective_date,
    expiration_date,
    inception_date,
    policy_status_code,
    policy_status_desc,
    bill_type_code,
    written_premium_amt,
    annualized_premium_amt,
    producer_source_sid,
    account_manager_source_sid,
    active_ind,
    policy_level_source_code,
    original_policy_id,
    program_desc_01,
    multi_year_ind,
    etl_update_datetime,
    etl_process_name,
    CASE 
        WHEN policy_source_sid IS NOT NULL THEN TRUE 
        ELSE FALSE 
    END AS etl_active_ind
FROM source_data

{% if is_incremental() %}
WHERE etl_update_datetime > (SELECT MAX(etl_update_datetime) FROM {{ this }})
{% endif %}
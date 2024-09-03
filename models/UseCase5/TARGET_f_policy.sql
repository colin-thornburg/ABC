-- models/mart/fct_edw_f_policy_EPIC_US.sql

{{ config(materialized='incremental', unique_key='policy_key') }}

WITH source_data AS (
    SELECT *
    FROM {{ ref('edw_f_policy_EPIC_US_insertion_status') }}
)

SELECT
    policy_key,
    client_key,
    carrier_insurer_key,
    carrier_payee_key,
    product_key,
    product_line_key,
    producer_01_key,
    producer_02_key,
    account_manager_key,
    effective_date_key,
    -- ... (include all other columns)
    policy_occurrence_code,
    insert_to_fact,
    insert_to_error,
    current_timestamp() as dbt_updated_at
FROM source_data
WHERE insert_to_fact = TRUE

{% if is_incremental() %}
    AND (
        policy_key NOT IN (SELECT policy_key FROM {{ this }})
        OR dbt_updated_at > (SELECT max(dbt_updated_at) FROM {{ this }})
    )
{% endif %}
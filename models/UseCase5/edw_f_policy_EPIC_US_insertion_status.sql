-- models/intermediate/int_edw_f_policy_EPIC_US_insertion_status.sql

{{ config(materialized='ephemeral') }}

WITH source_data AS (
    SELECT *
    FROM {{ ref('int_edw_f_policy_EPIC_US_main') }}
),

key_validations AS (
    SELECT 
        *,
        -- Key validations
        CASE WHEN CAST(policy_key AS VARCHAR) = CAST('unknown_key' AS VARCHAR) THEN TRUE ELSE FALSE END AS policy_key_error,
        CASE WHEN CAST(client_key AS VARCHAR) = CAST('unknown_key' AS VARCHAR) THEN TRUE ELSE FALSE END AS client_key_error,
        -- ... (include all other key validations)
        CASE WHEN CAST(policy_occurrence_key as VARCHAR) = CAST('unknown_key' AS VARCHAR) THEN TRUE ELSE FALSE END AS policy_occurrence_key_error
    FROM source_data
),

insertion_flags AS (
    SELECT 
        *,
        CASE
            WHEN NOT policy_key_error THEN TRUE
            ELSE FALSE
        END AS insert_to_fact,
        CASE
            WHEN policy_key_error
            OR client_key_error
            -- ... (include all other error conditions)
            OR policy_occurrence_key_error
                THEN TRUE
            ELSE FALSE
        END AS insert_to_error
    FROM key_validations
)

SELECT * FROM insertion_flags
-- models/staging/stg_policy_EPIC_US_renewal.sql

{{ config(materialized='ephemeral') }}

-- Placeholder for sp_load_temp_edw_f_policy_xxx_renewal_policy_1
SELECT
    policy_key,
    NULL AS renewal_policy_key
FROM {{ ref('stg_policy_EPIC_US_main') }}
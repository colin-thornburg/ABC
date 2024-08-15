{{ config(materialized='view') }}

SELECT
    extract_key,
    extract_name,
    extract_date,
    agency_system_name,
    bill_type_key,
    source_system_instance_code,
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM {{ ref('s_dim_extract') }}
{{ config(materialized='view') }}

SELECT
    policy_key,
    extract_key,
    client_key,
    department_key,
    effective_date,
    expiration_date,
    inception_date,
    product_line_key,
    bill_type_key,
    office_agency_system_key,
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM {{ ref('s_dim_policy_UC5') }}
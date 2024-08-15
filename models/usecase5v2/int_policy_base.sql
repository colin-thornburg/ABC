{{
    config(
        materialized='view'
    )
}}

WITH policy_base AS (
    SELECT
        p.policy_key,
        p.extract_key,
        p.client_key,
        p.department_key,
        p.effective_date,
        p.expiration_date,
        p.inception_date,
        p.product_line_key,
        b.agency_system_name,
        b.bill_type_key,
        b.source_system_instance_code,
        b.extract_date
    FROM {{ ref('stg_os1_fdw__s_dim_policy') }} p
    INNER JOIN {{ ref('stg_os1_fdw__s_dim_extract') }} b
        ON p.extract_key = b.extract_key
    WHERE p.office_agency_system_key = 'EPIC_US' --this will be a variable
    
)
SELECT 
   *
FROM policy_base
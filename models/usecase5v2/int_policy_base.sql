{{ config(materialized='incremental', unique_key='policy_key') }}

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
        p.bill_type_key,
        e.extract_date
    FROM {{ ref('stg_os1_fdw__s_dim_policy') }} p
    INNER JOIN {{ ref('stg_os1_fdw__s_dim_extract') }} e
        ON p.extract_key = e.extract_key
    WHERE p.office_agency_system_key = '{{ var("FDW_oas_id_EPIC_US") }}'
    {% if is_incremental() %}
        AND GREATEST(p.effective_date, p.inception_date, e.extract_date) > (SELECT MAX(inception_date) FROM {{ this }})
    {% endif %}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY p.policy_key
        ORDER BY e.extract_date DESC, p.policy_key DESC
    ) = 1
)
SELECT 
    policy_key,
    client_key,
    department_key,
    effective_date,
    expiration_date,
    inception_date,
    product_line_key,
    bill_type_key,
    extract_date AS last_extracted_date,
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM policy_base
{{ config(materialized='incremental', unique_key='policy_key') }}

SELECT
    p.policy_key,
    p.client_key,
    p.department_key,
    p.effective_date,
    p.expiration_date,
    p.inception_date,
    p.product_line_key,
    p.bill_type_key,
    COALESCE(x.bu_id, '{{ var("unknown_bu_id") }}') AS bu_id,
    p.last_extracted_date,
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM {{ ref('int_policy_base') }} p
LEFT JOIN {{ ref('stg_os1_fdw__s_dim_department') }} d
    ON p.department_key = d.department_key
LEFT JOIN {{ ref('stg_mdm__s_department_to_bu_xref') }} x
    ON TRIM(d.department_code) = TRIM(x.department_code)
{% if is_incremental() %}
WHERE p.last_extracted_date > (SELECT MAX(last_extracted_date) FROM {{ this }})
{% endif %}
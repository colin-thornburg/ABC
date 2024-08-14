{{ config(materialized='view') }}

SELECT
    department_key,
    department_code,
    department_name,
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM {{ ref('s_dim_department') }}
{{ config(materialized='view') }}

SELECT
    department_code,
    bu_id,
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM {{ ref('s_department_to_bu_xref') }}
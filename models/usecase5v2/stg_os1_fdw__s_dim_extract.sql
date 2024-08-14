{{ config(materialized='view') }}

SELECT
    extract_key,
    extract_name,
    extract_date,
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM {{ ref('s_dim_extract') }}
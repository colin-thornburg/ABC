{{ config(materialized='view') }}

SELECT
    date_key,
    date_value,
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM {{ ref('d_date') }}
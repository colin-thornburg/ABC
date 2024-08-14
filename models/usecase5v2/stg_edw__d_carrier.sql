{{ config(materialized='view') }}

SELECT
    carrier_key,
    carrier_name,
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM {{ ref('d_carrier') }}
{{ config(
    materialized='incremental',
    unique_key=['division_code', 'item_code', 'cdc_timestamp'],
    strategy='append'
) }}

WITH source_data AS (
    SELECT * FROM {{ ref('int_item_transformed') }}
    {% if is_incremental() %}
    WHERE cdc_timestamp > (SELECT MAX(cdc_timestamp) FROM {{ this }})
    {% endif %}
)

SELECT 
    *,
    CURRENT_TIMESTAMP() AS dbt_loaded_at
FROM source_data
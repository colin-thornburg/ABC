{{
    config(
      materialized='incremental',
      unique_key='business_key',
      full_refresh=false,
      on_schema_change='sync_all_columns'
    )
}}

-- Selecting data from the base_model
SELECT 
    *
FROM 
    {{ ref('dedup_model') }}

-- Unioning data from source4
UNION ALL
SELECT 
    *
FROM 
    {{ ref('source4') }}

-- Unioning data from source5
UNION ALL
SELECT 
    *
FROM 
    {{ ref('source5') }}

{% if is_incremental() %}
  -- this filter will only be applied on an incremental run
  WHERE Load_Timestamp > (SELECT MAX(Load_Timestamp) FROM {{ this }})
{% endif %}

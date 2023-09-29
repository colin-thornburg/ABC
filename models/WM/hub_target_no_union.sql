-- depends on {{ref('source1')}}
-- depends on {{ref('source2')}}
-- depends on {{ref('source3')}}
-- depends on {{ref('source4')}}
-- depends on {{ref('source5')}}

{{
    config(
      materialized='incremental',
      on_schema_change='sync_all_columns'
    )
}}

-- Selecting data from the base_model
SELECT 
    *
FROM 
    {{ ref(var("source_model")) }}


{% if is_incremental() %}
  -- this filter will only be applied on an incremental run

   WHERE business_key NOT IN (SELECT business_key FROM {{ this }}) AND
       Load_Timestamp > (SELECT MAX(Load_Timestamp) FROM {{ this }})
{% endif %}

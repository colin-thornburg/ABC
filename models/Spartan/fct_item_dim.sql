{{ config(
    materialized='incremental',
    unique_key=['division_code', 'item_code', 'cdc_timestamp'],
    strategy='append'
) }}

-- This model creates a type 2 table with a simple incremental model...
WITH data AS (
    SELECT
        int.item_key AS ITEM_PK,
        int.division_code AS FACILITY_PK,
        int.item_code AS ITEM_NBR,
        int.department,
        int.item_group,
        int.subgroup,
        int.gpc_merchandise_class_code AS MDSE_CLASS_KEY,
        CURRENT_TIMESTAMP() AS LAST_UPDT_TMSP,
        'MDM' AS ORIGIN
    FROM {{ ref('int_item_transformed') }} int
    
    {% if is_incremental() %}
    WHERE int.effective_from > (SELECT MAX(START_DT) FROM {{ this }})
    {% endif %}
)
-- Select all the latest data
SELECT * FROM data

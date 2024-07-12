{{ config(
    materialized='incremental',
    unique_key=['ITEM_PK', 'START_DT'],
    incremental_strategy='merge',
    merge_update_columns = ['END_DT', 'CURRENT_FLG', 'LAST_UPDT_TMSP']
) }}

-- This model creates the final ITEM_DIM table, combining data from hub and satellite tables
WITH latest_data AS (
    SELECT
        hub.item_key AS ITEM_PK,
        hub.division_code AS FACILITY_PK,
        hub.item_code AS ITEM_NBR,
        hub.department,
        hub.item_group,
        hub.subgroup,
        hub.gpc_merchandise_class_code AS MDSE_CLASS_KEY,
        sat.shelf_life,
        sat.case_upc_code AS UPC_CASE,
        sat.unit_upc_code AS upc_unit,
        sat.code_date_flag,
        sat.code_date_max,
        sat.code_date_min,
        sat.item_status,
        sat.item_description AS ITEM_DESCRIP,
        sat.brand,
        sat.vendor_code,
        sat.list_cost,
        sat.suggested_retail_price AS ISI_CPR_SRP,
        sat.master_pack, test CI
        sat.first_ship_date AS ISI_FIRST_SHIP_DATE,
        sat.average_wholesale_price AS AVG_WHOLESALE_PRICE,
        sat.shipper_type AS SHIPPER_FLAG,
        sat.hide_on_insite_flag AS INSITE_FLG,
        sat.effective_from AS START_DT,
        sat.effective_to AS END_DT,
        CASE WHEN sat.effective_to = '9999-12-31'::timestamp THEN 1 ELSE 0 END AS CURRENT_FLG,
        hub.created_at AS CREATE_TMSP,
        CURRENT_TIMESTAMP() AS LAST_UPDT_TMSP,
        'MDM' AS ORIGIN
    FROM {{ ref('hub_item') }} hub
    JOIN {{ ref('sat_item') }} sat ON hub.item_key = sat.item_key
    {% if is_incremental() %}
    WHERE sat.effective_from > (SELECT MAX(START_DT) FROM {{ this }})
    {% endif %}
)
-- Select all the latest data
SELECT * FROM latest_data

{{ config(
    materialized='incremental',
    unique_key=['ITEM_PK', 'START_DT'],
    incremental_strategy='merge',
    merge_update_columns = ['END_DT', 'CURRENT_FLG']
) }}

WITH hub_data AS (
    SELECT * FROM {{ ref('hub_item') }}
),

sat_data AS (
    SELECT * FROM {{ ref('sat_item') }}
    {% if is_incremental() %}
    WHERE effective_from > (SELECT MAX(START_DT) FROM {{ this }})
    {% endif %}
),

combined_data AS (
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
        sat.master_pack,
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
    FROM hub_data hub
    JOIN sat_data sat ON hub.item_key = sat.item_key
)

SELECT * FROM combined_data

{% if is_incremental() %}
UNION ALL

SELECT
    ITEM_PK,
    FACILITY_PK,
    ITEM_NBR,
    department,
    item_group,
    subgroup,
    MDSE_CLASS_KEY,
    shelf_life,
    UPC_CASE,
    upc_unit,
    code_date_flag,
    code_date_max,
    code_date_min,
    item_status,
    ITEM_DESCRIP,
    brand,
    vendor_code,
    list_cost,
    ISI_CPR_SRP,
    master_pack,
    ISI_FIRST_SHIP_DATE,
    AVG_WHOLESALE_PRICE,
    SHIPPER_FLAG,
    INSITE_FLG,
    START_DT,
    CASE 
        WHEN END_DT > (SELECT MIN(START_DT) FROM combined_data)
        THEN DATEADD(day, -1, (SELECT MIN(START_DT) FROM combined_data))
        ELSE END_DT
    END AS END_DT,
    CASE 
        WHEN END_DT > (SELECT MIN(START_DT) FROM combined_data)
        THEN 0
        ELSE CURRENT_FLG
    END AS CURRENT_FLG,
    CREATE_TMSP,
    CURRENT_TIMESTAMP() AS LAST_UPDT_TMSP,
    ORIGIN
FROM {{ this }}
WHERE CURRENT_FLG = 1
  AND ITEM_PK IN (SELECT ITEM_PK FROM combined_data)
{% endif %}
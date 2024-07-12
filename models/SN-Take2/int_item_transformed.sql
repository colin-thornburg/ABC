{{ config(materialized='view') }}

WITH stg_item AS (
    SELECT * FROM {{ ref('stg_item') }}
)

SELECT
    division_code,
    item_code,
    shelf_life,
    case_upc_code,
    unit_upc_code,
    code_date_flag,
    code_date_max,
    code_date_min,
    item_status,
    create_date,
    update_date,
    item_description,
    brand,
    vendor_code,
    list_cost,
    suggested_retail_price,
    master_pack,
    first_ship_date,
    department,
    item_group,
    subgroup,
    gpc_merchandise_class_code,
    average_wholesale_price,
    shipper_type,
    hide_on_insite_flag,
    cdc_timestamp,
    header__operation
FROM stg_item
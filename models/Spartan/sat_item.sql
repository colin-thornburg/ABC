{{ config(materialized='incremental', unique_key=['division_code', 'item_code', 'effective_from']) }}

-- The primary purpose of a satellite table in a Data Vault model is to track historical changes to attributes over time. 
-- By including effective_from in the unique key, we ensure that we can store multiple versions of the 
-- same business entity (identified by division_code and item_code) with different effective dates.

WITH int_item AS (
    SELECT * FROM {{ ref('int_item_transformed') }}
    WHERE header__operation != 'BEFOREIMAGE'
    {% if is_incremental() %}
    AND cdc_timestamp > (SELECT MAX(effective_from) FROM {{ this }})
    {% endif %}
),

satellite_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['division_code', 'item_code']) }} AS item_key,
        division_code,
        item_code,
        shelf_life,
        case_upc_code,
        unit_upc_code,
        code_date_flag,
        code_date_max,
        code_date_min,
        item_status,
        item_description,
        brand,
        vendor_code,
        list_cost,
        suggested_retail_price,
        master_pack,
        first_ship_date,
        average_wholesale_price,
        shipper_type,
        hide_on_insite_flag,
        cdc_timestamp AS effective_from,

        -- some fanciness to identify the current record
        LEAD(cdc_timestamp) OVER (
            PARTITION BY division_code, item_code 
            ORDER BY cdc_timestamp
        ) AS effective_to,
        cdc_timestamp AS load_date,
        header__operation AS record_source
    FROM int_item
)

SELECT 
    item_key,
    division_code,
    item_code,
    shelf_life,
    case_upc_code,
    unit_upc_code,
    code_date_flag,
    code_date_max,
    code_date_min,
    item_status,
    item_description,
    brand,
    vendor_code,
    list_cost,
    suggested_retail_price,
    master_pack,
    first_ship_date,
    average_wholesale_price,
    shipper_type,
    hide_on_insite_flag,
    effective_from,
    COALESCE(effective_to, '9999-12-31'::timestamp) AS effective_to,
    load_date,
    record_source
FROM satellite_data
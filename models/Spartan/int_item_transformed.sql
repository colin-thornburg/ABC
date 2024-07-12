{{ config(materialized='table') }}

WITH stg_item AS (
    SELECT * FROM {{ ref('stg_item') }}
)

SELECT
    -- Existing fields
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
    header__operation,

    -- Additional fields from staging
    master_case_length,
    master_case_width,
    master_case_height,
    master_case_weight,
    shipping_case_length,
    shipping_case_width,
    shipping_case_height,
    shipping_case_weight,
    item_size_descrip,
    short_description,
    replace_item_nbr,
    item_size,
    item_size_uom,
    gtin_nbr,
    root_item_nbr,
    cool_primary,
    cool_country_code,
    vendor_tier,
    store_pack_case,

    -- Calculated fields (moved from staging to intermediate for potential complex logic)
    CASE
        WHEN master_case_length IS NOT NULL AND master_case_width IS NOT NULL AND master_case_height IS NOT NULL THEN
            (master_case_length * master_case_width * master_case_height) / 1728
        ELSE NULL
    END AS master_case_cube,

    CASE
        WHEN shipping_case_length IS NOT NULL AND shipping_case_width IS NOT NULL AND shipping_case_height IS NOT NULL THEN
            (shipping_case_length * shipping_case_width * shipping_case_height) / 1728
        ELSE NULL
    END AS shipping_case_cube,

    -- Additional calculated fields or transformations can be added here
    -- For example, you might want to standardize some fields or create derived attributes

    -- Example: Standardized item status
    CASE
        WHEN UPPER(item_status) IN ('ACTIVE', 'A') THEN 'Active'
        WHEN UPPER(item_status) IN ('INACTIVE', 'I') THEN 'Inactive'
        ELSE 'Unknown'
    END AS standardized_item_status,

    -- Example: Derived attribute for item age (if relevant)
    DATEDIFF(day, create_date, CURRENT_DATE()) AS item_age_days,

    -- Example: Flag for items with missing crucial information
    CASE
        WHEN item_description IS NULL OR brand IS NULL OR vendor_code IS NULL THEN TRUE
        ELSE FALSE
    END AS missing_crucial_info_flag

FROM stg_item
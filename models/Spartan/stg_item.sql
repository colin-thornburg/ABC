{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ ref('NF_ITEM_ct_Sample') }}
),

renamed AS (
    SELECT
        -- Existing fields (as in your current model)
        header__change_seq,
        header__operation,
        header__timestamp AS cdc_timestamp,
        TRIM(ISI_DC_CODE) AS division_code,
        TRIM(ISI_ITEM_CODE) AS item_code,
        TRIM(ARTRAC_ARTDLIM) AS shelf_life,
        TRIM(ARTCOCA_UPC_ARCCODE) AS case_upc_code,
        TRIM(ARTCOUL_UPC_ACUCODE) AS unit_upc_code,
        TRIM(ISI_CODE_DATE_FLG) AS code_date_flag,
        ISI_CODE_DATE_MAX AS code_date_max,
        ISI_CODE_DATE_MIN AS code_date_min,
        TRIM(ISI_STATUS) AS item_status,
        ISI_DATE_CREATE AS create_date,
        ISI_DATE_UPDATE AS update_date,
        TRIM(ISI_ITEM_DESC) AS item_description,
        TRIM(ISI_BRAND) AS brand,
        TRIM(ISI_VENDOR_CODE) AS vendor_code,
        ISI_LIST_COST AS list_cost,
        ISI_CPR_SRP AS suggested_retail_price,
        ISI_MASTER_PACK AS master_pack,
        DATE(ISI_FIRST_SHIP_DATE) AS first_ship_date,
        TRIM(ISI_DEPARTMENT) AS department,
        TRIM(ISI_GROUP) AS item_group,
        TRIM(ISI_SUBGROUP) AS subgroup,
        TRIM(ISI_GPC_MDSE_CLS_CODE) AS gpc_merchandise_class_code,
        ISI_AWP_AMT AS average_wholesale_price,
        TRIM(ISI_SHIPPER_TYPE) AS shipper_type,
        CASE WHEN TRIM(UPPER(ISI_DO_NOT_SHOW_ON_INSITE)) = 'Y' THEN TRUE ELSE FALSE END AS hide_on_insite_flag,
        
        -- Additional fields
        ARTUL_PACK_ARULONG AS master_case_length,
        ARTUL_PACK_ARULARG AS master_case_width,
        ARTUL_PACK_ARUHAUT AS master_case_height,
        ARTUL_PACK_ARUPBRU AS master_case_weight,
        ARTUL_SPCK_ARULONG AS shipping_case_length,
        ARTUL_SPCK_ARULARG AS shipping_case_width,
        ARTUL_SPCK_ARUHAUT AS shipping_case_height,
        ARTUL_SPCK_ARUPBRU AS shipping_case_weight,
        TRIM(ISI_SIZE_UNIT) AS item_size_descrip,
        TRIM(ISI_POS_DESC) AS short_description,
        TRIM(ISI_REPLACE_ITEM) AS replace_item_nbr,
        ISI_WHOLESALE_SIZE AS item_size,
        TRIM(ISI_WHOLESALE_UOM) AS item_size_uom,
        TRIM(ARTCOUL_GTIN_ACUCODE) AS gtin_nbr,
        TRIM(ARTUV_ARVCEXV) AS root_item_nbr,
        TRIM(ISI_COOL_ID) AS cool_primary,
        TRIM(ISI_COOL) AS cool_country_code,
        ARTULUL_PALLET_ALLCOEFF AS vendor_tier,
        ARTULUL_SPCK_ALLCOEFF AS store_pack_case,
        
        -- Calculated fields
        (ARTUL_PACK_ARULONG * ARTUL_PACK_ARULARG * ARTUL_PACK_ARUHAUT) / 1728 AS master_case_cube,
        (ARTUL_SPCK_ARULONG * ARTUL_SPCK_ARULARG * ARTUL_SPCK_ARUHAUT) / 1728 AS shipping_case_cube

    FROM source
),

final AS (
    SELECT 
        *,
        {{ dbt_utils.generate_surrogate_key(['division_code', 'item_code']) }} AS item_key
    FROM renamed
)

SELECT * FROM final
ORDER BY division_code, item_code, cdc_timestamp

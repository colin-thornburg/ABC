{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ ref('NF_ITEM_ct_Sample') }}
),

renamed AS (
    SELECT
        -- CDC fields
        header__change_seq,
        header__operation,
        header__timestamp AS cdc_timestamp,
        
        -- Key identifiers
        TRIM(ISI_DC_CODE) AS division_code,
        TRIM(ISI_ITEM_CODE) AS item_code,
        
        -- SCD Type 2 tracked fields
        TRIM(ARTRAC_ARTDLIM) AS shelf_life,
        TRIM(ARTCOCA_UPC_ARCCODE) AS case_upc_code,
        TRIM(ARTCOUL_UPC_ACUCODE) AS unit_upc_code,
        TRIM(ISI_CODE_DATE_FLG) AS code_date_flag,
        ISI_CODE_DATE_MAX AS code_date_max,
        ISI_CODE_DATE_MIN AS code_date_min,
        
        -- Other fields
        TRIM(ISI_STATUS) AS item_status,
        ISI_DATE_CREATE AS create_date,
        ISI_DATE_UPDATE AS update_date,
        TRIM(ISI_ITEM_DESC) AS item_description,
        TRIM(ISI_BRAND) AS brand,
        TRIM(ISI_VENDOR_CODE) AS vendor_code,
        ISI_LIST_COST AS list_cost,
        ISI_CPR_SRP AS suggested_retail_price,
        ISI_MASTER_PACK AS master_pack,
        DATE(ISI_FIRST_SHIP_DATE) AS first_ship_date,  -- Changed to use DATE() function
        
        -- Additional fields that might be needed
        TRIM(ISI_DEPARTMENT) AS department,
        TRIM(ISI_GROUP) AS item_group,
        TRIM(ISI_SUBGROUP) AS subgroup,
        TRIM(ISI_GPC_MDSE_CLS_CODE) AS gpc_merchandise_class_code,
        ISI_AWP_AMT AS average_wholesale_price,
        TRIM(ISI_SHIPPER_TYPE) AS shipper_type,
        CASE WHEN TRIM(UPPER(ISI_DO_NOT_SHOW_ON_INSITE)) = 'Y' THEN TRUE ELSE FALSE END AS hide_on_insite_flag

    FROM source
),

final AS (
    SELECT 
        *,
        {{ dbt_utils.generate_surrogate_key(['division_code', 'item_code']) }} AS item_key
    FROM renamed
)

SELECT * FROM final order by division_code, item_code, CDC_TIMESTAMP
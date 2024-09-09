-- models/staging/stg_item.sql
{{ config(materialized='view') }}

SELECT
    ISI_DC_CODE AS facility_pk,
    ISI_ITEM_CODE AS item_nbr,
    ISI_STATUS,
    ISI_DATE_CREATE,
    ISI_DATE_UPDATE,
    ISI_ITEM_DESC AS item_descrip,
    ISI_SIZE_UNIT AS item_size_descrip,
    ISI_POS_DESC AS short_description,
    ISI_STRAIGHT_PACK AS straight_pack,
    ISI_REASON AS new_item_reason_cd,
    ISI_GPC_MDSE_CLS_CODE AS mdse_class_key,
    ISI_WHOLESALE_SIZE AS item_size,
    ISI_WHOLESALE_UOM AS item_size_uom,
    ISI_REPLACE_ITEM AS replace_item_nbr,
    ISI_SHIPPER_TYPE AS shipper_flag,
    ISI_BRAND AS brand,
    ISI_LV_DESCRIPTION AS lv_desc,
    ISI_ROOT_ITEM_DESC AS root_desc,
    ISI_CPR_SRP,
    ISI_CPR_EXT,
    ISI_CREATED_BY,
    ISI_FIRST_SHIP_DATE,
    ISI_MSA_CODE AS msa_cat_code,
    ISI_STICK_COUNT AS msa_stick_count,
    ISI_INNERPACK_SU,
    ISI_INNERPACK_COEF,
    ISI_INNERPACK_DESC,
    ISI_INNERPACK_SIZE,
    ISI_INNERPACK_UOM,
    ISI_INNERPACK_UPC,
    ISI_ITEM_BRAND_CD AS brand_cd,
    ISI_POS_16_DESC,
    ISI_RETAIL_ITEM_DESC,
    ISI_GPC_MDSE_CLS_DESC,
    ISI_SHIP_CASE_CNT,
    ISI_SHIP_UNIT_CD,
    ISI_SSRP_AMNT,
    ISI_SSRP_UNIT,
    ISI_CODE_DATE_FLG,
    ISI_CODE_DATE_MAX,
    ISI_CODE_DATE_MIN,
    ISI_DO_NOT_SHOW_ON_INSITE AS insite_flg,
    ISI_AWP_AMT,
    CURRENT_TIMESTAMP AS valid_from,
    '9999-12-31'::DATE AS valid_to
FROM {{ ref('NF_ITEM_ct_Sample') }}
-- models/marts/item_dim.sql
{{
    config(
        materialized='incremental',
        unique_key=['facility_pk', 'item_nbr'],
        on_schema_change='sync_all_columns'
    )
}}

SELECT
    facility_pk,
    item_nbr,
    item_descrip,
    item_size_descrip,
    short_description,
    straight_pack,
    new_item_reason_cd,
    mdse_class_key,
    item_size,
    item_size_uom,
    replace_item_nbr,
    shipper_flag,
    brand,
    lv_desc,
    root_desc,
    isi_cpr_srp,
    isi_cpr_ext,
    isi_created_by,
    isi_first_ship_date,
    msa_cat_code,
    msa_stick_count,
    isi_innerpack_su,
    isi_innerpack_coef,
    isi_innerpack_desc,
    isi_innerpack_size,
    isi_innerpack_uom,
    isi_innerpack_upc,
    brand_cd,
    isi_pos_16_desc,
    isi_retail_item_desc,
    isi_gpc_mdse_cls_desc,
    isi_ship_case_cnt,
    isi_ship_unit_cd,
    isi_ssrp_amnt,
    isi_ssrp_unit,
    isi_code_date_flg,
    isi_code_date_max,
    isi_code_date_min,
    insite_flg,
    isi_awp_amt,
    valid_from AS start_dt,
    valid_to AS end_dt,
    CASE WHEN valid_to = '9999-12-31' THEN 1 ELSE 0 END AS current_flg,
    'MDM' AS origin,
    CURRENT_TIMESTAMP AS create_tmsp,
    NULL AS last_updt_tmsp,
    TRY_TO_NUMBER(REGEXP_REPLACE(item_nbr, '\\D')) AS item_nbr_num,
    TRY_TO_NUMBER(REGEXP_REPLACE(replace_item_nbr, '\\D')) AS replace_item_nbr_num
FROM {{ ref('stg_item') }}

{% if is_incremental() %}
WHERE valid_from > (SELECT MAX(start_dt) FROM {{ this }})
{% endif %}
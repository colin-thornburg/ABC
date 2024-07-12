-- This model stages the raw data from the seed file NF_ITEM_ct_SAMPLE.csv.
-- It performs initial selection, renaming of columns for clarity, and basic transformations such as trimming whitespace and handling null values.

{{ config(
    materialized='view'
) }}

with source as (

    -- Select from the seed file defined as a source
    select
        header__change_seq,
        header__change_oper,
        header__change_mask,
        header__stream_position,
        header__operation,
        header__transaction_id,
        header__timestamp,
        ISI_DC_CODE,
        ISI_ITEM_CODE,
        ISI_DATE_UPDATE,
        ISI_LAST_PROG,
        ISI_BICEPS_READ,
        ISI_BICEPS_EXISTS,
        ISI_BICEPS_READ_TIME,
        ISI_BICEPS_TRANID,
        ISI_AWP_AMT,
        ISI_DO_NOT_SHOW_ON_INSITE,
        ISI_CODE_DATE_FLG,
        ISI_CODE_DATE_MAX,
        ISI_CODE_DATE_MIN,
        ISI_ASIN_ACUCODE,
        ISI_ITEM_RES09,
        ISI_DSS_EXISTS,
        ISI_DSS_READ,
        ISI_DSS_READ_TIME,
        ISI_DSS_TRANID,
        ARTRAC_ARTDLIM,
        ARTCOCA_UPC_ARCCODE,
        ARTCOUL_UPC_ACUCODE
    from {{ ref('NF_ITEM_ct_Sample') }}

)

-- Apply basic transformations and renaming
select
    header__change_seq as change_seq,
    header__change_oper as change_oper,
    header__change_mask as change_mask,
    header__stream_position as stream_position,
    header__operation as operation,
    header__transaction_id as transaction_id,
    header__timestamp as timestamp,
    ISI_DC_CODE as dc_code,
    ISI_ITEM_CODE as item_code,
    ISI_DATE_UPDATE as date_update,
    ISI_LAST_PROG as last_prog,
    ISI_BICEPS_READ as biceps_read,
    ISI_BICEPS_EXISTS as biceps_exists,
    ISI_BICEPS_READ_TIME as biceps_read_time,
    ISI_BICEPS_TRANID as biceps_tranid,
    ISI_AWP_AMT as awp_amt,
    ISI_DO_NOT_SHOW_ON_INSITE as do_not_show_on_insite,
    ISI_CODE_DATE_FLG as code_date_flg,
    ISI_CODE_DATE_MAX as code_date_max,
    ISI_CODE_DATE_MIN as code_date_min,
    ISI_ASIN_ACUCODE as asin_acucode,
    ISI_ITEM_RES09 as item_res09,
    ISI_DSS_EXISTS as dss_exists,
    ISI_DSS_READ as dss_read,
    ISI_DSS_READ_TIME as dss_read_time,
    ISI_DSS_TRANID as dss_tranid,
    ARTRAC_ARTDLIM as ardl_artdlm,
    ARTCOCA_UPC_ARCCODE as upc_arccode,
    ARTCOUL_UPC_ACUCODE as upc_acucode
from source

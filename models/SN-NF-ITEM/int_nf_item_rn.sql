-- This model applies ROW_NUMBER() to partition data by ISI_DC_CODE and ISI_ITEM_CODE, ordered by ISI_DATE_UPDATE.
-- It creates a row number column for subsequent joins in the intermediate and final layers.
-- It ensures all necessary columns for Type 2 tracking are included.

{{ config(
    materialized='view'
) }}

with staged_data as (

    -- Select from the staging model
    select
        change_seq,
        change_oper,
        change_mask,
        stream_position,
        operation,
        transaction_id,
        timestamp,
        dc_code,
        item_code,
        date_update,
        last_prog,
        biceps_read,
        biceps_exists,
        biceps_read_time,
        biceps_tranid,
        awp_amt,
        do_not_show_on_insite,
        code_date_flg,
        code_date_max,
        code_date_min,
        asin_acucode,
        item_res09,
        dss_exists,
        dss_read,
        dss_read_time,
        dss_tranid,
        ardl_artdlm,
        upc_arccode,
        upc_acucode
    from {{ ref('stg_nf_item_ct') }}

)

-- Apply ROW_NUMBER() to partition by ISI_DC_CODE and ISI_ITEM_CODE, ordered by ISI_DATE_UPDATE
select
    staged_data.*,
    row_number() over (
        partition by dc_code, item_code
        order by date_update desc
    ) as row_num
from staged_data

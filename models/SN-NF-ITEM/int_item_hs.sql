-- This model transforms ISI_ITEM_CODE to compute ITEM_NBR_HS and related fields.
-- It applies transformations like substring operations and padding to derive item numbers.
-- It ensures all necessary columns for Type 2 tracking are included.

{{ config(
    materialized='view' 
) }}

with row_numbered_data as (

    -- Select from the row numbering model
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
        upc_acucode,
        row_num
    from {{ ref('int_nf_item_rn') }}

)

-- Apply transformations to compute ITEM_NBR_HS and related fields
select
    row_numbered_data.*,
    case 
        when len(dc_code) = 1 then substring(item_code, 3)
        else substring(item_code, len(dc_code) + 1)
    end as item_nbr,
    
    len(case 
            when len(dc_code) = 1 then substring(item_code, 3)
            else substring(item_code, len(dc_code) + 1)
        end) as item_nbr_len,

    substring(case 
            when len(dc_code) = 1 then substring(item_code, 3)
            else substring(item_code, len(dc_code) + 1)
        end, 1, 1) as item_nbr_1,
    
    substring(case 
            when len(dc_code) = 1 then substring(item_code, 3)
            else substring(item_code, len(dc_code) + 1)
        end, 2, 1) as item_nbr_2,

    cast(substring(case 
            when len(dc_code) = 1 then substring(item_code, 3)
            else substring(item_code, len(dc_code) + 1)
        end, 1, 1) as int) * 2 as item_cd_int

from row_numbered_data

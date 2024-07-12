-- This model creates the satellite table by selecting detailed attributes and linking them to the hub table.
-- It stores historical tracking for changes over time, including all necessary Type 2 tracking columns.

{{ config(
    materialized='view'  
) }}

with hub_data as (

    -- Select from the hub model
    select
        dc_code,
        item_code,
        date_update,
        row_num
    from {{ ref('hub_nf_item') }}

),

item_hs as (

    -- Select from the item number transformation model
    select
        dc_code,
        item_code,
        date_update,
        row_num,
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
        operation,
        change_seq,
        change_oper,
        change_mask,
        stream_position,
        transaction_id,
        timestamp,
        ardl_artdlm,
        upc_arccode,
        upc_acucode
    from {{ ref('int_item_hs') }}

)

-- Select detailed attributes and link to the hub table
select
    hub_data.dc_code,
    hub_data.item_code,
    hub_data.date_update,
    hub_data.row_num,
    item_hs.last_prog,
    item_hs.biceps_read,
    item_hs.biceps_exists,
    item_hs.biceps_read_time,
    item_hs.biceps_tranid,
    item_hs.awp_amt,
    item_hs.do_not_show_on_insite,
    item_hs.code_date_flg,
    item_hs.code_date_max,
    item_hs.code_date_min,
    item_hs.asin_acucode,
    item_hs.item_res09,
    item_hs.dss_exists,
    item_hs.dss_read,
    item_hs.dss_read_time,
    item_hs.dss_tranid,
    item_hs.operation,
    item_hs.change_seq,
    item_hs.change_oper,
    item_hs.change_mask,
    item_hs.stream_position,
    item_hs.transaction_id,
    item_hs.timestamp,
    item_hs.ardl_artdlm,
    item_hs.upc_arccode,
    item_hs.upc_acucode
from hub_data
join item_hs
on hub_data.dc_code = item_hs.dc_code
and hub_data.item_code = item_hs.item_code
and hub_data.date_update = item_hs.date_update
and hub_data.row_num = item_hs.row_num

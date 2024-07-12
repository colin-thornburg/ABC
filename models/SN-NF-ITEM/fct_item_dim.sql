-- This model creates the final fact table by merging data from the hub and satellite tables.
-- It consolidates intermediate transformations and prepares data for final loading into the target EDW table (ITEM_DIM).

{{ config(
    materialized='incremental',
    unique_key=['item_code', 'dc_code'],
    strategy='merge'
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

satellite_data as (

    -- Select from the satellite model
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
    from {{ ref('sat_nf_item') }}

)

select
    hub_data.dc_code,
    hub_data.item_code,
    satellite_data.last_prog,
    satellite_data.biceps_read,
    satellite_data.biceps_exists,
    satellite_data.biceps_read_time,
    satellite_data.biceps_tranid,
    satellite_data.awp_amt,
    satellite_data.do_not_show_on_insite,
    satellite_data.code_date_flg,
    satellite_data.code_date_max,
    satellite_data.code_date_min,
    satellite_data.asin_acucode,
    satellite_data.item_res09,
    satellite_data.dss_exists,
    satellite_data.dss_read,
    satellite_data.dss_read_time,
    satellite_data.dss_tranid,
    satellite_data.operation,
    satellite_data.change_seq,
    satellite_data.change_oper,
    satellite_data.change_mask,
    satellite_data.stream_position,
    satellite_data.transaction_id,
    satellite_data.timestamp,
    satellite_data.ardl_artdlm,
    satellite_data.upc_arccode,
    satellite_data.upc_acucode,
    current_timestamp as load_timestamp
from hub_data
join satellite_data
on hub_data.dc_code = satellite_data.dc_code
and hub_data.item_code = satellite_data.item_code
and hub_data.date_update = satellite_data.date_update
and hub_data.row_num = satellite_data.row_num

{% if is_incremental() %}

  -- This filter will only be applied on an incremental run
  where satellite_data.timestamp >= (
      select coalesce(max(timestamp), '1900-01-01') from {{ this }}
  )

{% endif %}

-- This model creates the hub table by selecting unique business keys from the intermediate model.
-- It stores historical attributes and maintains type 2 slowly changing dimensions.

{{ config(
    materialized='view'
) }}

with item_hs as (

    -- Select from the item number transformation model
    select
        *
    from {{ ref('int_item_hs') }}

)

-- Select unique business keys and historical attributes
select distinct
    dc_code,
    item_code,
    item_nbr,
    item_nbr_len,
    item_nbr_1,
    item_nbr_2,
    item_cd_int,
    date_update,
    operation,
    change_seq,
    change_oper,
    change_mask,
    stream_position,
    transaction_id,
    timestamp,
    row_num
from item_hs

-- models/staging/stg_whnonstk.sql
{{ config(materialized='view') }}
SELECT
    item_id,
    iline as item_num,  // consistent join key
    description,
    load_date
FROM {{ ref('whnonstk') }}
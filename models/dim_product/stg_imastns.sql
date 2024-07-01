-- models/staging/stg_imastns.sql
{{ config(materialized='view') }}
SELECT
    item_id,
    iline as item_num,  -- renaming for consistent join key
    description,
    load_date
FROM {{ ref('imastns') }}  -- referencing the seed file treated as a source
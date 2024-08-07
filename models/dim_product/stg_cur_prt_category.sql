{{ config(materialized='view') }}
SELECT
    category_id,
    line as item_num,  -- renaming for consistent join key
    category_name,
    category_description
FROM {{ ref('cur_prt_category') }}  -- referencing the seed file treated as a source

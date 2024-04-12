{{ config(materialized='view') }}
SELECT
    item_id,
    iline as item_num,  -- renaming for consistent join key
    description,
    load_date
FROM {{ ref('imaster') }}  -- referencing the seed file treated as a source

{{ config(materialized='view') }}
SELECT
    im.item_id,
    im.item_num,
    im.description,
    im.load_date,
    cat.category_name,
    cat.category_description
FROM {{ ref('int_products_union') }} im
LEFT JOIN {{ ref('stg_cur_prt_category') }} cat
    ON im.item_num = cat.item_num
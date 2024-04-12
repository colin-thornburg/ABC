-- models/intermediate/int_products_union.sql
{{ config(materialized='view') }}
SELECT
    item_id,
    item_num,
    description,
    load_date,
    'IMAST' as source
FROM {{ ref('stg_imaster') }}
UNION ALL
SELECT
    item_id,
    item_num,
    description,
    load_date,
    'IMASTNS' as source
FROM {{ ref('stg_imastns') }}

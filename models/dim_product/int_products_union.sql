-- models/intermediate/int_products_union.sql
{{ config(materialized='view') }}
WITH unified_data AS (
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

    UNION ALL

    SELECT
        item_id,
        item_num,
        description,
        load_date,
        'WHNONSTK' as source
    FROM {{ ref('stg_whnonstk') }}
),
ranked_data AS (
    SELECT *,
           -- Rank records based on the source; lower numbers are prioritized
           ROW_NUMBER() OVER (PARTITION BY item_num ORDER BY CASE source
                               WHEN 'IMASTNS' THEN 1  -- Default priority to IMASTNS
                               WHEN 'WHNONSTK' THEN 2  -- Change these values to adjust priorities
                               ELSE 3
                               END) as rn
    FROM unified_data
)
SELECT
    item_id,
    item_num,
    description,
    load_date,
    source
FROM ranked_data
WHERE rn = 1  -- Filter to keep only the top-ranked records per item_num
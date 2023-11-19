-- int_level3_final_transformations.sql
SELECT
    *,
    -- Add any required calculations or CASE statements here
FROM {{ ref('int_level2_union_model') }}
